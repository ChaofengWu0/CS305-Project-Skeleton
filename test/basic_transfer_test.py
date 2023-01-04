import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import select
import util.simsocket as simsocket
import struct
import socket
import util.bt_utils as bt_utils
import hashlib
import argparse
import pickle
import time
import logging

"""
This is CS305 project skeleton code.
Please refer to the example files - example/dumpreceiver.py and example/dumpsender.py - to learn how to play with this skeleton.
"""

BUF_SIZE = 1400
HEADER_LEN = struct.calcsize("HBBHHII20s")
CHUNK_DATA_SIZE = 512 * 1024
MAX_PAYLOAD = 1024

config = None
ex_sending_chunkhash = ""

sampleRTT = 0
estimatedRTT = 0
devRTT = 0
timerDict = {}
ack_received = {}
ack_received2 = {}
ack_received3 = {}
LOG_FILENAME = "test.txt"
logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO)

dic = {}

test = "11111111111111111111".encode()


def process_download(sock, chunkfile, outputfile):
    # chunk1 ||| chunk2 ||| chunk3
    # chunk1, chunk2, chunk3
    # have_chunks = ""
    # for i in chunks:
    #   if have i:
    #       have_chunks+=i
    # ihave += have_chunks
    '''
    if DOWNLOAD is used, the peer will keep getting files until it is done
    '''
    print('PROCESS DOWNLOAD SKELETON CODE CALLED.  Fill me in!')
    # print('PROCESS GET SKELETON CODE CALLED.  Fill me in! I\'ve been doing! (', chunkfile, ',     ', outputfile, ')')
    global ex_output_file
    global ex_received_chunk
    global ex_downloading_chunkhash

    global sampleRTT
    global estimatedRTT
    global devRTT
    global timerDict
    global ack_received
    global ack_received2
    global ack_received3
    # This is a dict that can record all pkts
    # For the pkt that can is being transported, the value of the key is data
    # For the pkt that can has been transported, the value of the key is 'OK'
    timerDict = {}
    sampleRTT = 0
    estimatedRTT = 0
    devRTT = 0
    ex_received_chunk = {}
    # 1. success  ->  have downloaded
    # 2. process  ->  during the process to download the chunk
    # 3. nothing  ->  not ask for yet

    # todo
    # set a new dict to record duplicated ACKs by different ACK_num and Chunkhash

    ex_output_file = outputfile
    # Step 1: read chunkhash to be downloaded from chunkfile
    download_hash = bytes()

    # whohas packets
    who_has_packets = []

    with open(chunkfile, 'r') as cf:
        # todo
        # we have to read the whole file by loops.
        # we also need to send WHOHAS
        # Once we get one chunkhash, send 'WHOHAS' pkt to all other peers, at the same time we need to initialize the state to 'nothing'
        index, datahash_str = cf.readline().strip().split(" ")

        ex_downloading_chunkhash = datahash_str

        # hex_str to bytes
        datahash = bytes.fromhex(datahash_str)
        download_hash = download_hash + datahash

        # Step2: make WHOHAS pkt
        # |2byte magic|1byte type |1byte team|
        # |2byte  header len  |2byte pkt len |
        # |      4byte  seq                  |
        # |      4byte  ack                  |

        # todo Attention the whole project we use pkt len as header len
        whohas_header = struct.pack("!HBBHHII20s", (52305), 35, 0, (HEADER_LEN),
                                    (HEADER_LEN + len(download_hash)), (0), (0),
                                    datahash_str.encode())
        whohas_pkt = whohas_header + download_hash
        who_has_packets.append(whohas_pkt)

        while datahash_str:
            contents = cf.readline().strip().split(" ")
            if len(contents) < 2:
                break
            index = contents[0]
            datahash_str = contents[1]
            ex_downloading_chunkhash = datahash_str
            # hex_str to bytes
            datahash = bytes.fromhex(datahash_str)
            download_hash = bytes()
            download_hash = download_hash + datahash

            # Step2: make WHOHAS pkt
            # |2byte magic|1byte type |1byte team|
            # |2byte  header len  |2byte pkt len |
            # |      4byte  seq                  |
            # |      4byte  ack                  |
            whohas_header = struct.pack("!HBBHHII20s", (52305), 35, 0, (HEADER_LEN),
                                        (HEADER_LEN + len(download_hash)), (0), (0),
                                        datahash_str.encode())
            whohas_pkt = whohas_header + download_hash
            who_has_packets.append(whohas_pkt)

    # Step3: flooding whohas to all peers in peer list
    peer_list = config.peers
    for p in peer_list:
        if int(p[0]) != config.identity:
            for pkt in who_has_packets:
                key = (p[1] + str(p[2]))
                timerDict[key] = (p[1], p[2], str(time.time()), pkt)
                sock.sendto(pkt, (p[1], int(p[2])))


def process_inbound_udp(sock):
    global chunk_dic
    global config
    # ex_sending_chunkhash is the hash code of the chunk which needs to be downloaded
    global ex_sending_chunkhash
    # Receive pkt
    pkt, from_addr = sock.recvfrom(BUF_SIZE)
    Magic, Team, Type, hlen, plen, Seq, Ack, the_hash = struct.unpack("!HBBHHII20s", pkt[:HEADER_LEN])
    data = pkt[HEADER_LEN:]
    key = (from_addr[0] + str(from_addr[1]))

    timerDict[key] = (from_addr[0], from_addr[1], 'OK', None)

    print("SKELETON CODE CALLED, FILL this!")
    if Type == 0:
        # logging.info("-------------------")
        # logging.info(config.port)
        # logging.info('I receive a WHOHAS pkt')
        # logging.info("-------------------")
        # received an WHOHAS pkt
        # see what chunk the sender has
        whohas_chunk_hash = data[:20]
        # bytes to hex_str        # chunkhash_str is the hash code of the chunk which needs to be downloaded
        chunkhash_str = bytes.hex(whohas_chunk_hash)
        ex_sending_chunkhash = chunkhash_str

        print(f"whohas: {chunkhash_str}, has: {list(config.haschunks.keys())}")
        if chunkhash_str in config.haschunks:
            # send back IHAVE pkt
            ihave_header = struct.pack("!HBBHHII20s", (52305), 35, 1, (HEADER_LEN),
                                       (HEADER_LEN + len(whohas_chunk_hash)), (0),
                                       (0), whohas_chunk_hash)
            ihave_pkt = ihave_header + whohas_chunk_hash
            transmit(sock, key, from_addr, ihave_pkt)



    elif Type == 1:
        # logging.info("-------------------")
        # logging.info(config.port)
        # logging.info('I receive a IHAVE pkt')
        # logging.info("-------------------")
        # received an IHAVE pkt
        # see what chunk the sender has
        global dic
        # todo
        # Once get 'IHAVE' pkt, set the state to process.
        # For all 'IHAVE' pkts, check the state, only if the state is 'nothing' can we send 'GET' pkt

        get_chunk_hash = data[:20]
        ex_received_chunk[bytes.hex(get_chunk_hash)] = 0
        # send back GET pkt
        get_header = struct.pack("!HBBHHII20s", (52305), 35, 2, (HEADER_LEN),
                                 (HEADER_LEN + len(get_chunk_hash)), (0), (0),
                                 get_chunk_hash)
        ip_port = (config.ip, config.port, get_chunk_hash)
        if (dic.get(ip_port) == None):
            dic[ip_port] = "GET"
            get_pkt = get_header + get_chunk_hash

            transmit(sock, key, from_addr, get_pkt)

        else:
            commit_header = struct.pack("!HBBHHII20s", (52305), 35, 127, (HEADER_LEN),
                                        (HEADER_LEN), (0), (0),
                                        get_chunk_hash)
            # todo Attention, here we specialize it
            timerDict[key] = (from_addr[0], from_addr[1], 'OK', commit_header)
            sock.sendto(commit_header, from_addr)

    elif Type == 2:
        # logging.info("-------------------")
        # logging.info(config.port)
        # logging.info('I receive a GET pkt')
        # logging.info("-------------------")
        # received a GET pkt

        head = 0
        for i in range(8):
            if head + MAX_PAYLOAD < len(config.haschunks[ex_sending_chunkhash]):
                chunk_data = config.haschunks[ex_sending_chunkhash][head:head + MAX_PAYLOAD]
                head += MAX_PAYLOAD
                data_header = struct.pack("!HBBHHII20s", 52305, 35, 3, HEADER_LEN,
                                          HEADER_LEN, i, 0, the_hash)
                data_pkt = data_header + chunk_data
                transmit(sock, key, from_addr, data_pkt)
            else:
                break

    elif Type == 3:
        logging.info('---------------------------------------------------')
        logging.info("type=3")
        # logging.info('pre length')
        # logging.info((ex_received_chunk[bytes.hex(the_hash)]))
        # logging.info('data length')
        # logging.info(len(data))
        # received a DATA pkt
        # logging.info('after length')
        # logging.info((ex_received_chunk[bytes.hex(the_hash)]))
        # if (Ack == 141):
        #     logging.info(Ack)
        from_address = from_addr[1]
        if (ack_received2.get(from_address) is None):
            ack_received2[from_address] = 0
        if (ack_received2[from_address] == Seq and Seq < 512):
            ack_received2[from_address] += 1
            ex_received_chunk[bytes.hex(the_hash)] += 1
            ex_received_chunk[(bytes.hex(the_hash), Seq)] = data
        else:
            ack_received2[from_address] = ack_received2[from_address]
        # logging.info(ack_received2[from_address])
        # logging.info(ex_received_chunk[bytes.hex(the_hash)])
        # logging.info(Seq)
        # logging.info(Ack)
        # send back ACK

        ack_pkt = struct.pack("!HBBHHII20s", (52305), 35, 4, (HEADER_LEN),
                              (HEADER_LEN),
                              Seq, ack_received2[from_address], (the_hash))
        transfer = {}
        transfer[ex_downloading_chunkhash]=bytes()
        transmit(sock, key, from_addr, ack_pkt)
        # see if finished
        if (ex_received_chunk[bytes.hex(the_hash)]) == 512:
            output_file_chunk = dict()
            ex_received_chunk[bytes.hex(the_hash)] = bytes()
            for i in range(512):
                transfer[bytes.hex(the_hash)] += ex_received_chunk[(bytes.hex(the_hash), i)]
            with open(ex_output_file, "wb") as wf:
                pickle.dump(transfer, wf)
            # add to this peer's haschunk:
            config.haschunks[bytes.hex(the_hash)] = ex_received_chunk[bytes.hex(the_hash)]
            sha1 = hashlib.sha1()
            # sha1.update(ex_received_chunk[bytes.hex(the_hash)])
            sha1.update(ex_received_chunk[bytes.hex(the_hash)])

            # you need to print "GOT" when finished downloading all chunks in a DOWNLOAD file
            # ex_received_chunk[bytes.hex(the_hash)] = 512
            print(f"GOT {ex_output_file}")

    elif Type == 4:
        # received an ACK pkt
        # reset get's timer
        ack_num = (Ack)
        from_address = from_addr[1]
        logging.info(Seq)
        logging.info(Ack)
        if (ack_received3.get(from_address) is None):
            ack_received3[from_address] = [1, 0]

        if (ack_received3[from_address][0] == Ack - 1):
            ack_received3[from_address][0] += 1
            ack_received3[from_address][1] = 1
        else:
            ack_received3[from_address][1] += 1
            # logging.info(ack_received3[from_address][1])
            # logging.info(ack_received3[from_address][0])

        if (Ack == 505):
            logging.info(Ack)
        if (ack_received3[from_address][1] == 3):
            # ack_num = socket.htonl(Ack)
            for i in range(8):
                ack_num = ack_received3[from_address][0] - 1 + i
                left = (ack_num) * MAX_PAYLOAD
                right = min((ack_num + 1) * MAX_PAYLOAD, CHUNK_DATA_SIZE)
                next_data = config.haschunks[ex_sending_chunkhash][left: right]
                data_header = struct.pack("!HBBHHII20s", socket.htons(52305), 35, 3, socket.htons(HEADER_LEN),
                                          socket.htons(HEADER_LEN + len(next_data)),
                                          ack_received3[from_address][0] + i,
                                          ack_received3[from_address][0],
                                          the_hash)
                next_data_pkt = data_header + next_data
                transmit(sock, key, from_addr, next_data_pkt)
        else:
            if (ack_received3[from_address][1] < 3):
                ack_num = ack_received3[from_address][0] - 1
                # duplicated ACKs should be examined and here
                if (ack_num) * MAX_PAYLOAD >= CHUNK_DATA_SIZE:
                    # finished
                    print(f"finished sending {ex_sending_chunkhash}")
                    pass
                else:
                    left = (ack_num + 8) * MAX_PAYLOAD
                    right = min((ack_num + 9) * MAX_PAYLOAD, CHUNK_DATA_SIZE)
                    next_data = config.haschunks[ex_sending_chunkhash][left: right]
                    # send next data

                    # todo (ack_num) ack_num represents the ack num
                    data_header = struct.pack("!HBBHHII20s", (52305), 35, 3, (HEADER_LEN),
                                              (HEADER_LEN + len(next_data)),
                                              (ack_received3[from_address][0] - 1 + 8),
                                              ack_received3[from_address][0],
                                              the_hash)
                    next_data_pkt = data_header + next_data

                    transmit(sock, key, from_addr, next_data_pkt)


def transmit(sock, key, from_addr, pkt):
    timerDict[key] = (from_addr[0], from_addr[1], str(time.time()), pkt)
    sock.sendto(pkt, from_addr)


def process_user_input(sock):
    command, chunkf, outf = input().split(' ')
    if command == 'DOWNLOAD':
        process_download(sock, chunkf, outf)
        logging.info("-------------------")
        logging.info(config.port)
        logging.info('DOWNLOAD')
        logging.info("-------------------")
    else:
        pass


def getTimeout():
    return 100


def peer_run(config):
    addr = (config.ip, config.port)
    sock = simsocket.SimSocket(config.identity, addr, verbose=config.verbose)

    try:
        while True:
            # retransmit(sock)
            ready = select.select([sock, sys.stdin], [], [], 0.1)
            read_ready = ready[0]
            if len(read_ready) > 0:
                if sock in read_ready:
                    process_inbound_udp(sock)
                if sys.stdin in read_ready:
                    process_user_input(sock)
            else:
                # No pkt nor input arrives during this period
                pass
    except KeyboardInterrupt:
        pass
    finally:
        sock.close()


def retransmit(sock):
    for cnt, timer in enumerate(timerDict):
        now_tuple = timerDict[timer]
        ip = now_tuple[0]
        port = int(now_tuple[1])
        if now_tuple[2] == 'OK':
            continue
        start_time = float(now_tuple[2])
        pkt = now_tuple[3]
        address = (ip, port)
        timeout = 0
        now_time = time.time()
        if config.timeout is None:
            timeout = getTimeout()
        if float(now_time) - start_time > 221:
            logging.info("i send a retransmit pkt")
            timerDict[timer] = (ip, int(port), str(now_time), pkt)
            # todo Attention, here maybe wrong, sendto()may should send data rather than now_list[cnt]
            sock.sendto(pkt, address)


if __name__ == '__main__':
    """
    -p: Peer list file, it will be in the form "*.map" like nodes.map.
    -c: Chunkfile, a dictionary dumped by pickle. It will be loaded automatically in bt_utils. The loaded dictionary has the form: {chunkhash: chunkdata}
    -m: The max number of peer that you can send chunk to concurrently. If more peers ask you for chunks, you should reply "DENIED"
    -i: ID, it is the index in nodes.map
    -v: verbose level for printing logs to stdout, 0 for no verbose, 1 for WARNING level, 2 for INFO, 3 for DEBUG.
    -t: pre-defined timeout. If it is not set, you should estimate timeout via RTT. If it is set, you should not change this time out.
        The timeout will be set when running test scripts. PLEASE do not change timeout if it set.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', type=str, help='<peerfile>     The list of all peers', default='nodes.map')
    parser.add_argument('-c', type=str, help='<chunkfile>    Pickle dumped dictionary {chunkhash: chunkdata}')
    parser.add_argument('-m', type=int, help='<maxconn>      Max # of concurrent sending')
    parser.add_argument('-i', type=int, help='<identity>     Which peer # am I?')
    parser.add_argument('-v', type=int, help='verbose level', default=0)
    parser.add_argument('-t', type=int, help="pre-defined timeout", default=0)
    args = parser.parse_args()

    # LOG_FILENAME = "/home/wuchaofneg/sustech_networking/CS305/log/log.log"
    # logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO)
    # logging.info(config.timeout)
    config = bt_utils.BtConfig(args)
    peer_run(config)
