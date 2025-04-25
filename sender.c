#include <signal.h>
#include <stdbool.h>
#include <getopt.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include <rte_byteorder.h>
#include <rte_log.h>
#include <rte_common.h>
#include <rte_config.h>
#include <rte_errno.h>
#include <rte_ethdev.h>
#include <rte_ip.h>
#include <rte_mbuf.h>
#include <rte_malloc.h>
#include <rte_ring.h>
#include <rte_udp.h>
#include <rte_random.h>
#include <rte_ether.h>
#include <rte_cycles.h>
#include <rte_version.h>

uint32_t nb_ports;
#define EXIT_FAILURE 1
#define RTE_TEST_RX_DESC_DEFAULT 1024
#define RTE_TEST_TX_DESC_DEFAULT 1024
static uint16_t nb_rxd = RTE_TEST_RX_DESC_DEFAULT;
static uint16_t nb_txd = RTE_TEST_TX_DESC_DEFAULT;

#define MEMPOOL_CACHE_SIZE 128
#define BULK_SIZE 64

#define MAX_RING_SIZE 1024
#define CONSTRUCT_UDP_RING_SIZE 8192

#define MAX_PKT_BURST 32

#define MAX_PKT_SIZE 9500
#define MBUF_SIZE (9018 + 128 + RTE_PKTMBUF_HEADROOM)

#define DEFAULT_PKT_LEN 512
#define DEFAULT_TOTAL_TRAFFIC "1M"

int RTE_LOGTYPE_TRAFFIC_GEN;
uint32_t TRAFFIC_GEN_LOG_LEVEL = RTE_LOG_DEBUG;
#define APP "traffic-gen"

static volatile bool force_quit;

/* Options for packet generation */
static uint16_t pkt_len = DEFAULT_PKT_LEN;
static uint64_t total_traffic = 10000000; /* Default 10M bytes */
static uint64_t total_packets = 0; 

/* Define math header format */
typedef struct __attribute__((packed)) {
    uint32_t flag; // 4 types
    uint64_t timestamp; // 8 bytes
} flag_header_t;

struct rte_mempool *traffic_pktmbuf_pool = NULL;
static struct rte_eth_dev_tx_buffer *tx_buffer;

/* Ethernet addresses of ports */
static struct rte_ether_addr src_mac_addr = {{0xB8, 0x3F, 0xD2, 0x54, 0xBC, 0x6A}};
static struct rte_ether_addr dst_mac_addr = {{0xB8, 0x3F, 0xD2, 0x19, 0x77, 0xEE}};

/* IP addresses */
static uint32_t src_ip = RTE_IPV4(192, 168, 100, 2);
static uint32_t dst_ip = RTE_IPV4(192, 168, 100, 105);

/* UDP ports */
static uint16_t src_port = 8000;
static uint16_t dst_port = 8000;

static struct rte_eth_conf port_conf = {
    .rxmode = {
        .max_lro_pkt_size = MAX_PKT_SIZE,
    },
    .txmode = {
        .mq_mode = RTE_ETH_MQ_TX_NONE,
    },
    .link_speeds = RTE_ETH_LINK_SPEED_AUTONEG,
};

void signal_handler(int signo)
{
    if (signo == SIGINT || signo == SIGTERM)
    {
        printf("\nSignal %d received, preparing to exit...\n", signo);
        force_quit = true;
    }
}

void print_eth_dev_info(int portid) {
    struct rte_eth_dev_info dev_info;
    rte_eth_dev_info_get(portid, &dev_info);

    printf("Port %d Info:\n", portid);
    printf("  RX offload capabilities: 0x%" PRIx64 "\n", dev_info.rx_offload_capa);
    printf("  TX offload capabilities: 0x%" PRIx64 "\n", dev_info.tx_offload_capa);
    printf("  Max RX queues: %u\n", dev_info.max_rx_queues);
    printf("  Max TX queues: %u\n", dev_info.max_tx_queues);
    printf("  Max MTU: %u\n", dev_info.max_mtu);
    printf("  Min MTU: %u\n", dev_info.min_mtu);
    printf("  Driver name: %s\n", dev_info.driver_name);
}

void init_port(int portid)
{
    int ret;
    struct rte_eth_rxconf rxq_conf;
    struct rte_eth_txconf txq_conf;
    struct rte_eth_dev_info dev_info;

    rte_log(RTE_LOG_DEBUG, RTE_LOGTYPE_TRAFFIC_GEN, "Initializing port %u...\n", portid);
    fflush(stdout);

    /* init port */
    rte_eth_dev_info_get(portid, &dev_info);

    print_eth_dev_info(portid);

    if (dev_info.tx_offload_capa & RTE_ETH_TX_OFFLOAD_MBUF_FAST_FREE)
        port_conf.txmode.offloads |= RTE_ETH_TX_OFFLOAD_MBUF_FAST_FREE;
    
    /* Configure checksum offloads if supported */
    if (dev_info.tx_offload_capa & RTE_ETH_TX_OFFLOAD_IPV4_CKSUM) {
        port_conf.txmode.offloads |= RTE_ETH_TX_OFFLOAD_IPV4_CKSUM;
    }
    if (dev_info.tx_offload_capa & RTE_ETH_TX_OFFLOAD_UDP_CKSUM) {
        port_conf.txmode.offloads |= RTE_ETH_TX_OFFLOAD_UDP_CKSUM;
    }

    ret = rte_eth_dev_configure(portid, 1, 1, &port_conf);
    if (ret < 0)
        rte_exit(EXIT_FAILURE, "Cannot configure device: err=%d, port=%u\n",
                 ret, portid);

    ret = rte_eth_dev_adjust_nb_rx_tx_desc(portid, &nb_rxd, &nb_txd);
    if (ret < 0)
        rte_exit(EXIT_FAILURE,
                 "Cannot adjust number of descriptors: err=%d, port=%u\n",
                 ret, portid);

    /* init one RX queue */
    fflush(stdout);
    rxq_conf = dev_info.default_rxconf;
    rxq_conf.offloads = port_conf.rxmode.offloads;
    ret = rte_eth_rx_queue_setup(portid, 0, nb_rxd,
                                 rte_eth_dev_socket_id(portid),
                                 &rxq_conf,
                                 traffic_pktmbuf_pool);
    if (ret < 0)
        rte_exit(EXIT_FAILURE, "rte_eth_rx_queue_setup:err=%d, port=%u\n",
                 ret, portid);

    /* init one TX queue on each port */
    fflush(stdout);
    txq_conf = dev_info.default_txconf;
    txq_conf.offloads = port_conf.txmode.offloads;
    ret = rte_eth_tx_queue_setup(portid, 0, nb_txd,
                                 rte_eth_dev_socket_id(portid),
                                 &txq_conf);
    if (ret < 0)
        rte_exit(EXIT_FAILURE, "rte_eth_tx_queue_setup:err=%d, port=%u\n",
                 ret, portid);

    /*init TX buffers*/
    tx_buffer = rte_zmalloc_socket("tx_buffer",
                                   RTE_ETH_TX_BUFFER_SIZE(MAX_PKT_BURST), 0,
                                   rte_eth_dev_socket_id(portid));
    if (tx_buffer == NULL)
        rte_exit(EXIT_FAILURE, "Cannot allocate TX buffer\n");

    ret = rte_eth_tx_buffer_init(tx_buffer, MAX_PKT_BURST);
    if (ret != 0)
        rte_exit(EXIT_FAILURE, "Cannot initialize TX buffer: err=%d\n", ret);
    
    /* Start device */
    ret = rte_eth_dev_start(portid);
    if (ret < 0)
        rte_exit(EXIT_FAILURE, "rte_eth_dev_start:err=%d, port=%u\n",
                 ret, portid);

    rte_log(RTE_LOG_DEBUG, RTE_LOGTYPE_TRAFFIC_GEN, "Initialize port %u done.\n", portid);
}

/* Create a single packet with the specified format */
static struct rte_mbuf *create_packet(uint32_t flag)
{
    struct rte_mbuf *m;
    struct rte_ether_hdr *eth_hdr;
    struct rte_ipv4_hdr *ip_hdr;
    struct rte_udp_hdr *udp_hdr;
    flag_header_t *flag_header;
    uint16_t pkt_data_len;
    uint16_t math_data_len;
    uint16_t udp_length;
    char *payload;

    /* Calculate header sizes and data length */
    math_data_len = sizeof(flag_header_t);
    udp_length = sizeof(struct rte_udp_hdr) + math_data_len;
    
    /* Add padding if needed to meet the minimum packet length */
    pkt_data_len = pkt_len;
    
    /* Allocate the packet */
    m = rte_pktmbuf_alloc(traffic_pktmbuf_pool);
    if (m == NULL)
        return NULL;
    
    /* Set up the packet size */
    rte_pktmbuf_append(m, pkt_data_len);
    
    /* Set up the Ethernet header */
    eth_hdr = rte_pktmbuf_mtod(m, struct rte_ether_hdr *);
    rte_ether_addr_copy(&src_mac_addr, &eth_hdr->src_addr);
    rte_ether_addr_copy(&dst_mac_addr, &eth_hdr->dst_addr);
    eth_hdr->ether_type = rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4);
    
    /* Set up the IP header */
    ip_hdr = (struct rte_ipv4_hdr *)(eth_hdr + 1);
    memset(ip_hdr, 0, sizeof(*ip_hdr));
    ip_hdr->version_ihl = RTE_IPV4_VHL_DEF;
    ip_hdr->type_of_service = 0;
    ip_hdr->total_length = rte_cpu_to_be_16(sizeof(struct rte_ipv4_hdr) + udp_length);
    ip_hdr->packet_id = rte_cpu_to_be_16(0);
    ip_hdr->fragment_offset = rte_cpu_to_be_16(0);
    ip_hdr->time_to_live = 64;
    ip_hdr->next_proto_id = 0xF9;
    ip_hdr->src_addr = rte_cpu_to_be_32(src_ip);
    ip_hdr->dst_addr = rte_cpu_to_be_32(dst_ip);
    ip_hdr->hdr_checksum = 0;
    
    /* Set up the UDP header */
    udp_hdr = (struct rte_udp_hdr *)(ip_hdr + 1);
    udp_hdr->src_port = rte_cpu_to_be_16(src_port);
    udp_hdr->dst_port = rte_cpu_to_be_16(dst_port);
    udp_hdr->dgram_len = rte_cpu_to_be_16(udp_length);
    udp_hdr->dgram_cksum = 0;
    
    /* Set up the math header */
    flag_header = (flag_header_t *)(udp_hdr + 1);
    flag_header->flag = rte_cpu_to_be_32(flag); // Set the flag value
    flag_header->timestamp = 0; // Set the timestamp
    
    
    /* If the packet length is greater than headers + math_header, fill the rest with padding */
    if (pkt_data_len > sizeof(struct rte_ether_hdr) + sizeof(struct rte_ipv4_hdr) + 
        sizeof(struct rte_udp_hdr) + sizeof(flag_header_t)) {
        
        payload = (char *)(flag_header + 1);
        size_t padding_size = pkt_data_len - (sizeof(struct rte_ether_hdr) + 
                             sizeof(struct rte_ipv4_hdr) + sizeof(struct rte_udp_hdr) + 
                             sizeof(flag_header_t));
        
        /* Fill the padding with a pattern */
        memset(payload, 0xAB, padding_size);
    }
    
    /* Set offload flags if hardware supports checksum calculation */
    m->l2_len = sizeof(struct rte_ether_hdr);
    m->l3_len = sizeof(struct rte_ipv4_hdr);
    m->l4_len = sizeof(struct rte_udp_hdr);
    m->ol_flags = RTE_MBUF_F_TX_IPV4 | RTE_MBUF_F_TX_IP_CKSUM | RTE_MBUF_F_TX_UDP_CKSUM;
    
    return m;
}

/* Convert traffic size string to number of packets */
static uint64_t parse_traffic_size(const char *size_str) {
    uint64_t size;
    char *endptr;
    
    size = strtoull(size_str, &endptr, 10);
    
    if (*endptr != '\0') {
        switch (*endptr) {
            case 'k':
            case 'K':
                size *= 1000;
                break;
            case 'm':
            case 'M':
                size *= 1000000;
                break;
            case 'g':
            case 'G':
                size *= 1000000000;
                break;
            default:
                fprintf(stderr, "Invalid traffic size suffix: %c\n", *endptr);
                exit(EXIT_FAILURE);
        }
    }
    
    return size;
}

static void usage(const char *prgname)
{
    printf("%s [EAL options] -- "
           "  -m PACKET_LENGTH: length of packets to transmit\n"
           "  -n TOTAL_TRAFFIC: total traffic to transmit (e.g., 10M)\n",
           prgname);
}

static int parse_args(int argc, char **argv)
{
    int opt;
    char **argvopt;
    int option_index;
    char *prgname = argv[0];
    static struct option lgopts[] = {
        {NULL, 0, 0, 0}
    };

    argvopt = argv;

    while ((opt = getopt_long(argc, argvopt, "m:n:",
                  lgopts, &option_index)) != EOF) {

        switch (opt) {
        case 'm':
            pkt_len = (uint16_t)atoi(optarg);
            if (pkt_len < 64 || pkt_len > MAX_PKT_SIZE) {
                printf("Invalid packet length: %u (must be between 64 and %u)\n",
                       pkt_len, MAX_PKT_SIZE);
                usage(prgname);
                return -1;
            }
            break;

        case 'n':
            total_traffic = parse_traffic_size(optarg);
            total_packets = total_traffic / pkt_len;
            // printf("Total traffic: %lu bytes, %lu packets\n", total_traffic, total_packets);
            if (total_packets == 0) {
                printf("Invalid total traffic: %s\n", optarg);
                usage(prgname);
                return -1;
            }
            break;

        default:
            usage(prgname);
            return -1;
        }
    }

    if (optind >= 0)
        argv[optind-1] = prgname;

    optind = 1; /* reset getopt lib */
    return 0;
}

static void traffic_generator_main_loop(uint16_t port_id)
{
    struct rte_mbuf *pkts[MAX_PKT_BURST];
    uint16_t sent;
    uint64_t packets_sent = 0;
    uint64_t total_sent = 0;
    uint64_t start_tsc, current_tsc, last_report_tsc;
    const uint64_t report_interval_tsc = rte_get_tsc_hz(); /* 1 second */
    float duration_sec;
    
    printf("Starting packet generation. Sending %lu packets of size %u bytes...\n", 
           total_packets, pkt_len);
    
    start_tsc = rte_rdtsc();
    last_report_tsc = start_tsc;
    
    while (!force_quit && total_sent < total_packets) {
        /* Create a burst of packets */
        uint16_t nb_pkts = RTE_MIN(MAX_PKT_BURST, total_packets - total_sent);
        
        for (int i = 0; i < nb_pkts; i++) {
            pkts[i] = create_packet();
            if (pkts[i] == NULL) {
                rte_exit(EXIT_FAILURE, "Failed to allocate packet\n");
            }
        }
        
        /* Send the burst */
        sent = rte_eth_tx_burst(port_id, 0, pkts, nb_pkts);
        packets_sent += sent;
        total_sent += sent;
        
        /* Free unsent packets if any */
        if (unlikely(sent < nb_pkts)) {
            for (uint16_t i = sent; i < nb_pkts; i++) {
                rte_pktmbuf_free(pkts[i]);
            }
        }
    }
    
    duration_sec = (float)(rte_rdtsc() - start_tsc) / rte_get_tsc_hz();
    printf("Traffic generation complete. Sent %lu packets in %.2f seconds (%.2f Mpps)\n", 
           total_sent, duration_sec, total_sent / 1000000.0 / duration_sec);
}

static int generator_launch_one_lcore(__attribute__((unused)) void *dummy)
{
    traffic_generator_main_loop(0); /* Always use first port */
    return 0;
}

int main(int argc, char **argv)
{
    int ret;
    uint32_t nb_lcores;
    uint64_t nb_mbufs;
    uint16_t portid = 0;
    unsigned int generator_lcore_id;
    
    /* Initialize the Environment Abstraction Layer (EAL) */
    ret = rte_eal_init(argc, argv);
    if (ret < 0)
        rte_exit(EXIT_FAILURE, "Invalid EAL arguments\n");
    argc -= ret;
    argv += ret;

    /* Parse application-specific arguments */
    ret = parse_args(argc, argv);
    if (ret < 0)
        rte_exit(EXIT_FAILURE, "Invalid application arguments\n");

    /* Register a log type for the application */
    RTE_LOGTYPE_TRAFFIC_GEN = rte_log_register(APP);
    ret = rte_log_set_level(RTE_LOGTYPE_TRAFFIC_GEN, TRAFFIC_GEN_LOG_LEVEL);
    if (ret < 0)
        rte_exit(EXIT_FAILURE, "Set log level to %u failed\n", TRAFFIC_GEN_LOG_LEVEL);

    /* Check that we have enough CPU cores */
    nb_lcores = rte_lcore_count();
    if (nb_lcores < 2)
        rte_exit(EXIT_FAILURE, "Number of CPU cores should be at least 2.");

    /* Check if at least one port is available */
    nb_ports = rte_eth_dev_count_avail();
    if (nb_ports == 0)
        rte_exit(EXIT_FAILURE, "No Ethernet ports available\n");

    rte_log(RTE_LOG_DEBUG, RTE_LOGTYPE_TRAFFIC_GEN, "%u port(s) available\n", nb_ports);
    rte_log(RTE_LOG_DEBUG, RTE_LOGTYPE_TRAFFIC_GEN, "DPDK version: %s\n", rte_version());

    /* Setup signal handlers */
    force_quit = false;
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    /* Create mbuf pool */
    nb_mbufs = RTE_MAX((unsigned int)(nb_rxd + nb_txd + MAX_PKT_BURST + MEMPOOL_CACHE_SIZE), 8192U);
    traffic_pktmbuf_pool = rte_pktmbuf_pool_create("mbuf_pool", nb_mbufs,
                                                  MEMPOOL_CACHE_SIZE, 0, MBUF_SIZE,
                                                  rte_socket_id());
    if (traffic_pktmbuf_pool == NULL)
        rte_exit(EXIT_FAILURE, "Cannot init mbuf pool\n");

    /* Initialize port */
    init_port(portid);
    ret = rte_eth_dev_set_mtu(portid, MAX_PKT_SIZE);
    if (ret < 0)
        rte_exit(EXIT_FAILURE, "rte_eth_dev_set_mtu:err=%d, port=%u\n",
                 ret, portid);
    
    /* Get the MAC address of the port */
    rte_eth_macaddr_get(portid, &src_mac_addr);
    printf("Using MAC address: %02X:%02X:%02X:%02X:%02X:%02X\n",
           src_mac_addr.addr_bytes[0], src_mac_addr.addr_bytes[1],
           src_mac_addr.addr_bytes[2], src_mac_addr.addr_bytes[3],
           src_mac_addr.addr_bytes[4], src_mac_addr.addr_bytes[5]);

    /* Launch traffic generator on a slave core */
    generator_lcore_id = rte_get_next_lcore(rte_lcore_id(), true, false);
    if (rte_eal_remote_launch(generator_launch_one_lcore, NULL, generator_lcore_id) < 0)
        rte_exit(EXIT_FAILURE, "Cannot launch generator on lcore\n");

    /* Wait for traffic generator to complete */
    if (rte_eal_wait_lcore(generator_lcore_id) < 0)
        ret = -1;

    /* Display statistics */
    struct rte_eth_stats stats;
    rte_eth_stats_get(portid, &stats);
    printf("Port %u: RX-packets=%"PRIu64" TX-packets=%"PRIu64" RX-dropped=%"PRIu64" TX-dropped=%"PRIu64"\n", 
           portid, stats.ipackets, stats.opackets, stats.imissed, stats.oerrors);

    /* Cleanup */
    rte_eth_dev_stop(portid);
    rte_eth_dev_close(portid);
    
    rte_exit(EXIT_SUCCESS, "Traffic generation complete\n");
    return 0;
}