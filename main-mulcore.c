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
#include <rte_atomic.h>
#include <rte_lcore.h>

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

#define MAX_PKT_NUM 10000
#define NUM_WORKER_CORES 3  

int RTE_LOGTYPE_TRAFFIC_GEN;
uint32_t TRAFFIC_GEN_LOG_LEVEL = RTE_LOG_DEBUG;
#define APP "traffic-gen"

static volatile bool force_quit;

/* Options for packet generation */
static uint16_t pkt_len = DEFAULT_PKT_LEN;
// static uint64_t total_traffic = 10000000; /* Default 10M bytes */
static uint64_t total_packets = 0; 
// rte_atomic32_t pkt_idx;
struct rte_ring *worker_rings[NUM_WORKER_CORES];

/* Define math header format */
typedef struct __attribute__((packed)) {
    uint16_t a;
    uint16_t b;
    int32_t res;
    char timestamp[6]; /* 6 bytes for timestamp */
} math_header_t;


/* Information Collection array */
// math_header_t math_info[MAX_PKT_NUM];

struct rte_mempool *traffic_pktmbuf_pool = NULL;
static struct rte_eth_dev_tx_buffer *tx_buffer;

/* Ethernet addresses of ports */
static struct rte_ether_addr src_mac_addr = {{0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC}};
static struct rte_ether_addr dst_mac_addr = {{0xDE, 0xAD, 0xBE, 0xEF, 0x12, 0x34}};

/* IP addresses */
static uint32_t src_ip = RTE_IPV4(192, 168, 1, 1);
static uint32_t dst_ip = RTE_IPV4(192, 168, 1, 2);

/* UDP ports */
static uint16_t src_port = 12345;
static uint16_t dst_port = 54321;

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

static void init_rings()
{
    char ring_name[32];
    for (int i = 0; i < NUM_WORKER_CORES; i++) {
        snprintf(ring_name, sizeof(ring_name), "worker_ring_%d", i);
        worker_rings[i] = rte_ring_create(ring_name, MAX_RING_SIZE, rte_socket_id(), 
                                          RING_F_SP_ENQ | RING_F_SC_DEQ);
        if (worker_rings[i] == NULL)
            rte_exit(EXIT_FAILURE, "Cannot create ring %s\n", ring_name);
    }
}

static void delete_rings()
{
    for (int i = 0; i < NUM_WORKER_CORES; i++) {
        if (worker_rings[i] != NULL) {
            rte_ring_free(worker_rings[i]);
            worker_rings[i] = NULL;
        }
    }
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


/* RX core function - receives packets and distributes to worker cores */
static int rx_core_main_loop(uint16_t port_id)
{
    struct rte_mbuf *pkts[MAX_PKT_BURST];
    uint16_t received;
    uint64_t total_rx = 0;
    int worker_id = 0;
    
    printf("RX core started on lcore %u\n", rte_lcore_id());
    
    while (!force_quit) {
        /* receive packets */
        received = rte_eth_rx_burst(port_id, 0, pkts, MAX_PKT_BURST);
        if (received > 0) {
            total_rx += received;
            
            /* Distribute packets to worker cores in round-robin fashion */
            for (int i = 0; i < received; i++) {
                worker_id = (worker_id++) % NUM_WORKER_CORES;
                
                /* Try to enqueue the packet without blocking */
                if (rte_ring_enqueue(worker_rings[worker_id], pkts[i]) != 0) {
                    /* Ring full, free the packet */
                    rte_pktmbuf_free(pkts[i]);
                }
            }
        }
    }
    
    printf("RX core %u processed %lu packets\n", rte_lcore_id(), total_rx);
    return 0;
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

uint64_t transferring_timestamp(char *timestamp)
{
    uint64_t ts = 0;
    for (int i = 0; i < 6; i++)
    {
        ts = ts << 8;
        ts += (uint64_t)timestamp[i];
    }
    return ts;
}

/* analyze the information from the information collection array, and save in a txt file */
// static void analyze_info()
// {
//     FILE *fp = fopen("math_info.txt", "w");
//     if (fp == NULL)
//     {
//         printf("Error opening file for writing\n");
//         return;
//     }
//     uint32_t num_pkts = rte_atomic32_read(&pkt_idx);

//     for (int i = 0; i < num_pkts; i++)
//     {
//         fprintf(fp, "%d %d %d %lu\n", rte_be_to_cpu_16(math_info[i].a) , rte_be_to_cpu_16(math_info[i].b), rte_be_to_cpu_32(math_info[i].res), transferring_timestamp(math_info[i].timestamp));
//     }

//     fclose(fp);
// }

// static int parse_args(int argc, char **argv);


static void math_process_main_loop0(uint16_t port_id)
{
    struct rte_mbuf *pkts[MAX_PKT_BURST];
    uint16_t received;
    uint16_t worker_id = 0;
    

    
    while (!force_quit) {
        
        /* receive packets */
        received = rte_ring_dequeue_burst(worker_rings[worker_id], (void **)pkts, MAX_PKT_BURST, NULL);
        if (received == 0)
            continue;
        for (int i = 0; i < received; i++) {
            struct rte_ether_hdr *eth_hdr;
            struct rte_ipv4_hdr *ip_hdr;
            struct rte_udp_hdr *udp_hdr;
            math_header_t *math_hdr;

            eth_hdr = rte_pktmbuf_mtod(pkts[i], struct rte_ether_hdr *);
            ip_hdr = (struct rte_ipv4_hdr *)(eth_hdr + 1);
            if (ip_hdr->next_proto_id != 0xF9) {
                /* Not a UDP packet, free the mbuf */
                rte_pktmbuf_free(pkts[i]);
                continue;
            }
            udp_hdr = (struct rte_udp_hdr *)(ip_hdr + 1);
            math_hdr = (math_header_t *)(udp_hdr + 1);

            math_hdr->res = rte_be_to_cpu_16(math_hdr->a) + rte_be_to_cpu_16(math_hdr->b);

           /*send out the modified packet*/
            if(rte_eth_tx_burst(port_id, 0, &pkts[i], 1) == 0) {
                /* If the send fails, free the mbuf */
                rte_pktmbuf_free(pkts[i]);
            }
        }                      
    }
}

static void math_process_main_loop1(uint16_t port_id)
{
    struct rte_mbuf *pkts[MAX_PKT_BURST];
    uint16_t received;
    uint16_t worker_id = 1;
    

    
    while (!force_quit) {
        
        /* receive packets */
        received = rte_ring_dequeue_burst(worker_rings[worker_id], (void **)pkts, MAX_PKT_BURST, NULL);
        if (received == 0)
            continue;
            for (int i = 0; i < received; i++) {
                struct rte_ether_hdr *eth_hdr;
                struct rte_ipv4_hdr *ip_hdr;
                struct rte_udp_hdr *udp_hdr;
                math_header_t *math_hdr;
    
                eth_hdr = rte_pktmbuf_mtod(pkts[i], struct rte_ether_hdr *);
                ip_hdr = (struct rte_ipv4_hdr *)(eth_hdr + 1);
                if (ip_hdr->next_proto_id != 0xF9) {
                    /* Not a UDP packet, free the mbuf */
                    rte_pktmbuf_free(pkts[i]);
                    continue;
                }
                udp_hdr = (struct rte_udp_hdr *)(ip_hdr + 1);
                math_hdr = (math_header_t *)(udp_hdr + 1);
    
                math_hdr->res = rte_be_to_cpu_16(math_hdr->a) + rte_be_to_cpu_16(math_hdr->b);
    
               /*send out the modified packet*/
                if(rte_eth_tx_burst(port_id, 0, &pkts[i], 1) == 0) {
                    /* If the send fails, free the mbuf */
                    rte_pktmbuf_free(pkts[i]);
                }
            }   
                       
    }
}

static void math_process_main_loop2(uint16_t port_id)
{
    struct rte_mbuf *pkts[MAX_PKT_BURST];
    uint16_t received;
    uint16_t worker_id = 2;
    

    
    while (!force_quit) {
        
        /* receive packets */
        received = rte_ring_dequeue_burst(worker_rings[worker_id], (void **)pkts, MAX_PKT_BURST, NULL);
        if (received == 0)
            continue;
            for (int i = 0; i < received; i++) {
                struct rte_ether_hdr *eth_hdr;
                struct rte_ipv4_hdr *ip_hdr;
                struct rte_udp_hdr *udp_hdr;
                math_header_t *math_hdr;
    
                eth_hdr = rte_pktmbuf_mtod(pkts[i], struct rte_ether_hdr *);
                ip_hdr = (struct rte_ipv4_hdr *)(eth_hdr + 1);
                if (ip_hdr->next_proto_id != 0xF9) {
                    /* Not a UDP packet, free the mbuf */
                    rte_pktmbuf_free(pkts[i]);
                    continue;
                }
                udp_hdr = (struct rte_udp_hdr *)(ip_hdr + 1);
                math_hdr = (math_header_t *)(udp_hdr + 1);
    
                math_hdr->res = rte_be_to_cpu_16(math_hdr->a) + rte_be_to_cpu_16(math_hdr->b);
    
               /*send out the modified packet*/
                if(rte_eth_tx_burst(port_id, 0, &pkts[i], 1) == 0) {
                    /* If the send fails, free the mbuf */
                    rte_pktmbuf_free(pkts[i]);
                }
            }   
        }
}

static int rx_core_launch_one_lcore(__attribute__((unused)) void *dummy)
{
    rx_core_main_loop(0); /* Always use first port */
    return 0;
}

static int generator_launch_one_lcore0(__attribute__((unused)) void *dummy)
{
    math_process_main_loop0(0); /* Always use first port */
    return 0;
}

static int generator_launch_one_lcore1(__attribute__((unused)) void *dummy)
{
    math_process_main_loop1(0); /* Always use first port */
    return 0;
}

static int generator_launch_one_lcore2(__attribute__((unused)) void *dummy)
{
    math_process_main_loop2(0); /* Always use first port */
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

    /* Initialize rings for worker cores */
    init_rings();

    /* Launch traffic generator on a slave core */

    uint32_t rx_core_lcore_id = rte_get_next_lcore(rte_lcore_id(), true, false);
    if (rte_eal_remote_launch(rx_core_launch_one_lcore, NULL, rx_core_lcore_id) < 0)
        rte_exit(EXIT_FAILURE, "Cannot launch RX core on lcore\n");

    uint32_t generator_lcore_id0 = rte_get_next_lcore(rx_core_lcore_id, true, false);
    if (rte_eal_remote_launch(generator_launch_one_lcore0, NULL, generator_lcore_id0) < 0)
        rte_exit(EXIT_FAILURE, "Cannot launch generator0 on lcore\n");

    uint32_t generator_lcore_id1 = rte_get_next_lcore(generator_lcore_id0, true, false);
    if (rte_eal_remote_launch(generator_launch_one_lcore1, NULL, generator_lcore_id1) < 0)
        rte_exit(EXIT_FAILURE, "Cannot launch generator1 on lcore\n");

    uint32_t generator_lcore_id2 = rte_get_next_lcore(generator_lcore_id1, true, false);
    if (rte_eal_remote_launch(generator_launch_one_lcore2, NULL, generator_lcore_id2) < 0)
        rte_exit(EXIT_FAILURE, "Cannot launch generator2 on lcore\n");

    /* Wait for traffic generator to complete */
    if (rte_eal_wait_lcore(rx_core_lcore_id) < 0)
        ret = -1;

    if (rte_eal_wait_lcore(generator_lcore_id0) < 0)
        ret = -1;

    if (rte_eal_wait_lcore(generator_lcore_id1) < 0)
        ret = -1;

    if (rte_eal_wait_lcore(generator_lcore_id2) < 0)
        ret = -1;

    /* Display statistics */
    struct rte_eth_stats stats;
    rte_eth_stats_get(portid, &stats);
    printf("Port %u: RX-packets=%"PRIu64" TX-packets=%"PRIu64" RX-dropped=%"PRIu64" TX-dropped=%"PRIu64"\n", 
           portid, stats.ipackets, stats.opackets, stats.imissed, stats.oerrors);

    delete_rings();

    /* Cleanup */
    rte_eth_dev_stop(portid);
    rte_eth_dev_close(portid);
    
    rte_exit(EXIT_SUCCESS, "Traffic generation complete\n");
    return 0;
}

