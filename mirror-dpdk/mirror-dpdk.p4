/* -*- P4_16 -*- */

#include <core.p4> 
#include <tna.p4> 


const bit<10> MIRROR_SESSION_TO_MONITOR = 10w777;

/*************************************************************************
 **************  I N G R E S S   P R O C E S S I N G   *******************
 *************************************************************************/
 
    /***********************  H E A D E R S  ************************/
// header eg_mirror_h {
//     bit<16> fid;            // flow id
//     bit<16> pkt_len;
//     bit<64> arrival_time;   // arrival time to P4-2
//     bit<32> s_seq;          // sending sequence number foreach flow
//     bit<32> r_seq;          // receiving sequence number foreach flow
//     bit<64> ftv;            // ftv
//     bit<16> ifid;           // interface id on P4-4
// }

header ethernet_t {
    bit<48> dstAddr;
    bit<48> srcAddr;
    bit<16> etherType;
}

// Define the IPv4 header
header ipv4_t {
    bit<4>   version;
    bit<4>   ihl;
    bit<8>   diffserv;
    bit<16>  totalLen;
    bit<16>  identification;
    bit<3>   flags;
    bit<13>  fragOffset;
    bit<8>   ttl;
    bit<8>   protocol;
    bit<16>  hdrChecksum;
    bit<32>  srcAddr;
    bit<32>  dstAddr;
}

// Define the UDP header
header udp_t {
    bit<16> srcPort;
    bit<16> dstPort;
    bit<16> length;
    bit<16> checksum;
}

// Define the flag header
header flag_t {
   bit<32> flag;
    bit<64> timestamp;
}
// self-defined header to notify the other host
header noti_t {
    bit<32> flag;
    bit<32> notify;
    bit<64> timestamp;
}

struct my_ingress_headers_t {
    ethernet_t  ethernet;
    ipv4_t      ipv4;
    udp_t       udp;
    flag_t      flag;
}

    /******  G L O B A L   I N G R E S S   M E T A D A T A  *********/

struct my_ingress_metadata_t {
    bit<10> mirror_session;
}

    /***********************  P A R S E R  **************************/
parser IngressParser(packet_in        pkt,
    /* User */    
    out my_ingress_headers_t          hdr,
    out my_ingress_metadata_t         meta,
    /* Intrinsic */
    out ingress_intrinsic_metadata_t  ig_intr_md)
{
    // Checksum() tcp_csum;
    /* This is a mandatory state, required by Tofino Architecture */
     state start {
        pkt.extract(ig_intr_md);
        pkt.advance(PORT_METADATA_SIZE);
        transition parse_ethernet;
    }
    state parse_ethernet {
        hdr.ethernet.setValid();
        pkt.extract(hdr.ethernet);
        transition select(hdr.ethernet.etherType) {
            0x0800 : parse_ipv4;
            default : accept; // for ARP pkts
        }
    }

    state parse_ipv4 {
        hdr.ipv4.setValid();
        pkt.extract(hdr.ipv4);
        transition select(hdr.ipv4.protocol){
            0xf9: parse_flag_udp;
            // 0xfb: parse_noti;
            default: accept;
        }
    }

    state parse_flag_udp {
        hdr.udp.setValid();
        pkt.extract(hdr.udp);
        transition parse_flag;
    }

    state parse_flag {
        hdr.flag.setValid();
        pkt.extract(hdr.flag);
        transition accept;
    }

    // state parse_noti {
    //     hdr.noti.setValid();
    //     pkt.extract(hdr.noti);
    //     transition accept;
    // }

    
}

    /***************** M A T C H - A C T I O N  *********************/

control Ingress(
    /* User */
    inout my_ingress_headers_t                       hdr,
    inout my_ingress_metadata_t                      meta,
    /* Intrinsic */
    in    ingress_intrinsic_metadata_t               ig_intr_md,
    in    ingress_intrinsic_metadata_from_parser_t   ig_prsr_md,
    inout ingress_intrinsic_metadata_for_deparser_t  ig_dprsr_md,
    inout ingress_intrinsic_metadata_for_tm_t        ig_tm_md)
{

    action mirror_enabler(bit<10> eg_mir_ses) {
        meta.mirror_session = eg_mir_ses;
        ig_dprsr_md.mirror_type = 1;
    }

     action mirror_disabler(){
        ig_dprsr_md.mirror_type = 2;
     }


    action send(PortId_t port) {
        ig_tm_md.ucast_egress_port = port;
    }
    action drop() {
        ig_dprsr_md.drop_ctl = 1;
    }
     action stamp_tstamp() {
        hdr.flag.timestamp = (bit<64>)ig_intr_md.ingress_mac_tstamp;
    }

    
    
    apply {
        // 55/0 >> 136
        
        if(hdr.flag.isValid()){
            mirror_enabler(MIRROR_SESSION_TO_MONITOR);
        }
        else
        {
            mirror_disabler();
        }

        if(ig_intr_md.ingress_port == 128){
            send(144);
        }
        else if(ig_intr_md.ingress_port == 144){
            send(128);
        }
        else{
            if (ig_intr_md.ingress_port == 148)
            {
                if(hdr.ipv4.dstAddr == 0xC0A86468) // 192.168.100.104
                    send(144);
                else if(hdr.ipv4.dstAddr == 0xC0A86403) // 192.168.100.3
                    send(168);
                else
                    drop();
            }
        }
    }
}

    /*********************  D E P A R S E R  ************************/

control IngressDeparser(packet_out pkt,
    /* User */
    inout my_ingress_headers_t                       hdr,
    in    my_ingress_metadata_t                      meta,
    /* Intrinsic */
    in    ingress_intrinsic_metadata_for_deparser_t  ig_dprsr_md)
{
    Mirror() mirror;
    apply {
        if(ig_dprsr_md.mirror_type == 1){
            mirror.emit(
            meta.mirror_session
        );
        }
        
        pkt.emit(hdr.ethernet);
        pkt.emit(hdr.ipv4);
        pkt.emit(hdr.udp);
        pkt.emit(hdr.flag);
        // pkt.emit(hdr.noti);

    }        
}

/*************************************************************************
 ****************  E G R E S S   P R O C E S S I N G   *******************
 *************************************************************************/

    /***********************  H E A D E R S  ************************/

struct my_egress_headers_t {
    ethernet_t  ethernet;
    ipv4_t      ipv4;
    udp_t       udp;
    flag_t      flag;
    noti_t      noti;
    
}

    /********  G L O B A L   E G R E S S   M E T A D A T A  *********/

struct my_egress_metadata_t {
    // bit<10> mirror_session;
}

    /***********************  P A R S E R  **************************/

parser EgressParser(packet_in        pkt,
    /* User */
    out my_egress_headers_t          hdr,
    out my_egress_metadata_t         meta,
    /* Intrinsic */
    out egress_intrinsic_metadata_t  eg_intr_md)
{
    state start {
        pkt.extract(eg_intr_md);
        transition parse_ethernet;
    }

    state parse_ethernet {
        hdr.ethernet.setValid();
        pkt.extract(hdr.ethernet);
        transition select(hdr.ethernet.etherType) {
            0x0800 : parse_ipv4;
            default : accept; // for ARP pkts
        }
    }

    state parse_ipv4 {
        hdr.ipv4.setValid();
        pkt.extract(hdr.ipv4);
        transition select(hdr.ipv4.protocol){
            0xf9: parse_flag_udp;
            0xfa: parse_noti_udp;
            default: accept;
        }
    }
    state parse_flag_udp {
        hdr.udp.setValid();
        pkt.extract(hdr.udp);
        transition parse_flag;
    }

    state parse_noti_udp {
        hdr.udp.setValid();
        pkt.extract(hdr.udp);
        transition parse_noti;
    }

    state parse_flag {
        hdr.flag.setValid();
        pkt.extract(hdr.flag);
        transition accept;
    }
    state parse_noti {
        hdr.noti.setValid();
        pkt.extract(hdr.noti);
        transition accept;
    }
    

    
}

    /***************** M A T C H - A C T I O N  *********************/

control Egress(
    /* User */
    inout my_egress_headers_t                          hdr,
    inout my_egress_metadata_t                         meta,
    /* Intrinsic */    
    in    egress_intrinsic_metadata_t                  eg_intr_md,
    in    egress_intrinsic_metadata_from_parser_t      eg_prsr_md,
    inout egress_intrinsic_metadata_for_deparser_t     eg_dprsr_md,
    inout egress_intrinsic_metadata_for_output_port_t  eg_oport_md)
{
    action stamp_flag_tstamp() {
        hdr.flag.timestamp = (bit<64>)eg_prsr_md.global_tstamp;
    }
    action stamp_noti_tstamp() {
        hdr.noti.timestamp = (bit<64>)eg_prsr_md.global_tstamp;
    }
    apply {
    if(hdr.flag.isValid()){
        stamp_flag_tstamp();
    }
    if(hdr.noti.isValid()){
        stamp_noti_tstamp();
    }
    else{
    }
}
}

    /*********************  D E P A R S E R  ************************/

control EgressDeparser(packet_out pkt,
    /* User */
    inout my_egress_headers_t                       hdr,
    in    my_egress_metadata_t                      meta,
    /* Intrinsic */
    in    egress_intrinsic_metadata_for_deparser_t  eg_dprsr_md)
{
    // Mirror() mirror;
    apply {
        
        pkt.emit(hdr.ethernet);
        pkt.emit(hdr.ipv4);
        pkt.emit(hdr.udp);

        // 发出所有可能的字段，pkt.emit 会自动忽略无效字段
        pkt.emit(hdr.flag);
        pkt.emit(hdr.noti);
    }
}


/************ F I N A L   P A C K A G E ******************************/
Pipeline(
    IngressParser(),
    Ingress(),
    IngressDeparser(),
    EgressParser(),
    Egress(),
    EgressDeparser()
) pipe;

Switch(pipe) main;