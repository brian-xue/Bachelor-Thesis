#include <signal.h>
#include <stdbool.h>
#include <getopt.h>
#include <stdlib.h>

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
#include <rte_pcapng.h>
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

#define MAX_PKT_BURST 8

#define MAX_PKT_SIZE 9500
#define MBUF_SIZE (9018 + 128 + RTE_PKTMBUF_HEADROOM)

int pkt_loss = 0;
int pkt_counter1 = 0;
int pkt_counter2 = 0;

int RTE_LOGTYPE_ENDSYS;
uint32_t ENDSYS_LOG_LEVEL = RTE_LOG_DEBUG;
#define APP "end-sys"


static struct rte_eth_conf port_conf = {
    .rxmode = {
        .max_lro_pkt_size = MAX_PKT_SIZE,
        // .offloads = RTE_ETH_RX_OFFLOAD_KEEP_CRC,
        .mtu = 9000,
    },
    .txmode = {
        .mq_mode = RTE_ETH_MQ_TX_VMDQ_DCB,
    },
    .link_speeds = RTE_ETH_LINK_SPEED_AUTONEG,
};

struct rte_mempool *endsys_pktmbuf_pool = NULL;
static struct rte_eth_dev_tx_buffer *tx_buffer;

static volatile bool force_quit;

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

    rte_log(RTE_LOG_DEBUG, RTE_LOGTYPE_ENDSYS, "Initializing port %u...\n", portid);
    fflush(stdout);

    /* init port */
    rte_eth_dev_info_get(portid, &dev_info);

    print_eth_dev_info(portid);

    if (dev_info.rx_offload_capa & RTE_ETH_RX_OFFLOAD_KEEP_CRC) {
        port_conf.rxmode.offloads |= RTE_ETH_RX_OFFLOAD_KEEP_CRC;
    } else {
        printf("Port %u does not support RTE_ETH_RX_OFFLOAD_KEEP_CRC, disabling it.\n", portid);
        port_conf.rxmode.offloads &= ~RTE_ETH_RX_OFFLOAD_KEEP_CRC;
    }

    if (dev_info.rx_offload_capa & RTE_ETH_RX_OFFLOAD_CHECKSUM) {
        port_conf.rxmode.offloads &= ~RTE_ETH_RX_OFFLOAD_CHECKSUM; // 禁用 RX 校验和检查
    } else {
        printf("Port %u does not support RX checksum offload\n", portid);
    }
    if (dev_info.tx_offload_capa & RTE_ETH_TX_OFFLOAD_MBUF_FAST_FREE)
        port_conf.txmode.offloads |=
            RTE_ETH_TX_OFFLOAD_MBUF_FAST_FREE;

    
    ret = rte_eth_dev_configure(portid, 1, 1, &port_conf);
    if (ret < 0)
        rte_exit(EXIT_FAILURE, "Cannot configure device: err=%d, port=%u\n",
                 ret, portid);

    ret = rte_eth_dev_adjust_nb_rx_tx_desc(portid, &nb_rxd,
                                           &nb_txd);
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
                                 endsys_pktmbuf_pool);
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
    
    /* Start device */
    ret = rte_eth_dev_start(portid);
    if (ret < 0)
        rte_exit(EXIT_FAILURE, "rte_eth_dev_start:err=%d, port=%u\n",
                 ret, portid);

    ret = rte_eth_promiscuous_enable(portid);
    if(ret != 0)
        rte_exit(EXIT_FAILURE, "rte_eth_promiscuous_enable:err=%d, port=%u\n",
                 ret, portid);

    rte_log(RTE_LOG_DEBUG, RTE_LOGTYPE_ENDSYS, "Initilize port %u done.\n", portid);
}


void signal_handler(int signo)
{
    if (signo == SIGINT || signo == SIGTERM)
    {
        printf("\nSignal %d received, preparing to exit...\n", signo);
        force_quit = true;
    }
}

void endsys_usage(const char *prgname)
{
    printf("%s [EAL options] --"
           "\t-p PORTID: port to configure\n",
           prgname);
}


static void transmit_main_loop1()
{
    struct rte_mbuf *bufs[MAX_PKT_BURST];
    struct rte_mbuf *m;
    uint16_t nb_rx, nb_tx;
    uint16_t portid = 0;
    rte_pcapng_t *pcapng = NULL;
    struct rte_mbuf *pmbuf;

    FILE *fp = fopen("tx_port0.pcapng", "w");
    if(fp == NULL)
    {
        rte_exit(EXIT_FAILURE, "Cannot open pcapng file\n");
    }

    int fd = fileno(fp);


    rte_log(RTE_LOG_DEBUG, RTE_LOGTYPE_ENDSYS, "entering transmit loop1 on lcore %u\n", rte_lcore_id());
    
    pcapng = rte_pcapng_fdopen(fd, "Linux", "Intel", "DPDK", "This is a pcapng file for port0");

    while(!force_quit)
    {
        // rte_log(RTE_LOG_DEBUG, RTE_LOGTYPE_USER1, "Polling port %u...\n", portid);
        // fflush(stdout); // Ensure logs are flushed immediately for debugging
        // printf("Polling port %u...\n", portid);
        // fflush(stdout);
        nb_rx = rte_eth_rx_burst(portid, 0, bufs, MAX_PKT_BURST);
        pkt_counter1 += nb_rx;
        for(int i=0;i<nb_rx;i++)
            {
                m = bufs[i];
                printf("transmitting packet from port0 to port1\n");
                pmbuf = rte_pcapng_copy(0, 0, m, endsys_pktmbuf_pool, rte_pktmbuf_pkt_len(m), 0,RTE_PCAPNG_DIRECTION_OUT);
                rte_pcapng_write_packets(pcapng, &pmbuf, 1);
                // if(rte_eth_tx_burst(1, 0, &m, 1)<1)
                // {
                    rte_pktmbuf_free(m);
                    // pkt_loss++;
                // }
            }
    }
    rte_pcapng_close(pcapng);
}

static void transmit_main_loop2()
{
    struct rte_mbuf *bufs[MAX_PKT_BURST];
    struct rte_mbuf *m;
    uint16_t nb_rx, nb_tx;
    // uint16_t portid = 1;
    return;
    // rte_pcapng_t *pcapng = NULL;
    // struct rte_mbuf *pmbuf;
    // FILE *fp = fopen("tx_port1.pcapng", "w");
    // if(fp == NULL)
    // {
    //     rte_exit(EXIT_FAILURE, "Cannot open pcapng file\n");
    // }
    // int fd = fileno(fp);


    // rte_log(RTE_LOG_DEBUG, RTE_LOGTYPE_ENDSYS, "entering transmit loop2 on lcore %u\n", rte_lcore_id());
    
    // pcapng = rte_pcapng_fdopen(fd, "Linux", "Intel", "DPDK", "This is a pcapng file for port1");


    // while(!force_quit)
    // {
    //     nb_rx = rte_eth_rx_burst(portid, 0, bufs, MAX_PKT_BURST);
    //     pkt_counter2 += nb_rx;
    //     for(int i=0;i<nb_rx;i++)
    //         {
    //             m = bufs[i];
    //             // printf("transmitting packet from port1 to port0\n");
    //             pmbuf = rte_pcapng_copy(1, 0, m, endsys_pktmbuf_pool, rte_pktmbuf_pkt_len(m),0, RTE_PCAPNG_DIRECTION_OUT);
    //             rte_pcapng_write_packets(pcapng, &pmbuf, 1);
    //             if(rte_eth_tx_burst(0, 0, &m, 1)<1)
    //             {
    //                 rte_pktmbuf_free(m);
    //                 pkt_loss++;
    //             }
    //         }
    // }
    // rte_pcapng_close(pcapng);
}


static void transmit_pfc_pkts()
{
    struct rte_mbuf *bufs[MAX_PKT_BURST];
    struct rte_mbuf *m;
    uint16_t nb_rx, nb_tx;
    // uint16_t portid = 0;
    /* constructing and send pfc pkts */
}

static uint64_t convert_timestamp_format(char timestamp[])
{
    uint64_t ts = 0;
    for (int i = 0; i < 6; i++)
    {
        ts = ts << 8;
        ts += (uint64_t)timestamp[i];
    }
    return ts;
}


static int
transmit1_launch_one_lcore(__attribute__((unused)) void *dummy)
{
    transmit_main_loop1();
    return 0;
}

static int
transmit2_launch_one_lcore(__attribute__((unused)) void *dummy)
{
    transmit_main_loop2();
    return 0;
}


void main(int argc, char **argv)
{
    int ret;
    uint32_t nb_lcores;
    uint64_t nb_mbufs;
    uint16_t portid;
    unsigned int transmit_lcore_id1, transmit_lcore_id2;

    
    ret = rte_eal_init(argc, argv);
    if (ret < 0)
        rte_exit(EXIT_FAILURE, "Invalid EAL arguments\n");
    argc -= ret;
    argv += ret;

    RTE_LOGTYPE_ENDSYS = rte_log_register(APP);
    ret = rte_log_set_level(RTE_LOGTYPE_ENDSYS, ENDSYS_LOG_LEVEL);
    if (ret < 0)
        rte_exit(EXIT_FAILURE, "Set log level to %u failed\n", ENDSYS_LOG_LEVEL);

    nb_lcores = rte_lcore_count();
    printf("nb_lcores: %d\n", nb_lcores);
    if (nb_lcores < 7)
        rte_exit(EXIT_FAILURE, "Number of CPU cores should be no less than 7.");

    nb_ports = rte_eth_dev_count_avail();
    if (nb_ports == 0)
        rte_exit(EXIT_FAILURE, "No Ethernet ports, bye...\n");

    rte_log(RTE_LOG_DEBUG, RTE_LOGTYPE_ENDSYS, "%u port(s) available\n", nb_ports);

    rte_log(RTE_LOG_DEBUG, RTE_LOGTYPE_ENDSYS, "DPDK version: %s\n", rte_version());

    force_quit = false;
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    nb_ports = 1;
    portid = 0;

    nb_mbufs = RTE_MAX((unsigned int)(nb_ports * (nb_rxd + nb_txd + MAX_PKT_BURST + MEMPOOL_CACHE_SIZE)), 8192U);
    endsys_pktmbuf_pool = rte_pktmbuf_pool_create("mbuf_pool", nb_mbufs,
                                                    MEMPOOL_CACHE_SIZE, 0, MBUF_SIZE,
                                                    rte_socket_id());
    if (endsys_pktmbuf_pool == NULL)
        rte_exit(EXIT_FAILURE, "Cannot init mbuf pool\n");

    // for(portid = 0; portid < nb_ports; portid++)
    // {
        init_port(portid);
        ret = rte_eth_dev_set_mtu(portid, MAX_PKT_SIZE);
    if (ret < 0)
        rte_exit(EXIT_FAILURE, "rte_eth_dev_set_mtu:err=%d, port=%u\n",
                 ret, portid);
    // }

    

    transmit_lcore_id1 = rte_get_next_lcore(0, true, false);
    transmit_lcore_id2 = rte_get_next_lcore(transmit_lcore_id1, true, false);

    if(rte_eal_remote_launch(transmit1_launch_one_lcore, NULL, transmit_lcore_id1) < 0)
        rte_exit(EXIT_FAILURE, "Cannot launch transmit1 lcore\n");

    if(rte_eal_remote_launch(transmit2_launch_one_lcore, NULL, transmit_lcore_id2) < 0)
        rte_exit(EXIT_FAILURE, "Cannot launch transmit2 lcore\n");

    if(rte_eal_wait_lcore(transmit_lcore_id1) < 0)
        ret = -1;

    if(rte_eal_wait_lcore(transmit_lcore_id2) < 0)
        ret = -1;

    struct rte_eth_stats stats;

    // for(portid = 0; portid < nb_ports; portid++)
    // {
    //     rte_eth_stats_get(portid, &stats);
    //     printf("Port %u: RX-packets=%"PRIu64" TX-packets=%"PRIu64 " RX-dropped=%"PRIu64 " TX-dropped=%"PRIu64 "\n", portid, stats.ipackets, stats.opackets, stats.imissed, stats.oerrors);
    // }

    // for(portid = 0; portid < nb_ports; portid++)
    // {
        rte_eth_dev_stop(portid);
        rte_eth_dev_close(portid);
    // }
    printf("Packets loss: %d\n", pkt_loss);

    printf("Core 1 packet counter: %d\n", pkt_counter1);
    printf("Core 2 packet counter: %d\n", pkt_counter2);

    rte_exit(EXIT_SUCCESS, "Bye...\n");

}