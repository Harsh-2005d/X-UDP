# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
import struct
from concurrent.futures import ThreadPoolExecutor
import time
from hashlib import md5
class Streamer:
    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated recv_buffer loss."""
        self.HEADER_SIZE=21
        self.MAX_PAYLOAD = 1472 - self.HEADER_SIZE
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port
        self.recv_buffer={}
        self.next_seq=0
        self.send_seq=0
        self.closed=False
        self.ack=set()
        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(self.listener)
        self.fin_acked=False
        self.fin_rec=False

    def create_packet(self,seq,flag,chunk:bytes =b''):
        base_header = struct.pack("!IB", seq, flag)
        data_to_hash = base_header + chunk
        
        checksum = md5(data_to_hash).digest()
        
        full_header = struct.pack("!IB16s", seq, flag, checksum)
        return full_header + chunk

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        # Your code goes here!  The code below should be changed!
        
        for i in range(0, len(data_bytes), self.MAX_PAYLOAD):
            chunk = data_bytes[i : i + self.MAX_PAYLOAD]
            packet = self.create_packet(self.send_seq,0,chunk)
            while self.send_seq not in self.ack:
                self.socket.sendto(packet, (self.dst_ip, self.dst_port))
                
                # Wait up to 0.05s for the ACK
                start_time = time.time()
                while time.time() - start_time < 0.05 and self.send_seq not in self.ack:
                    time.sleep(0.01)
            self.send_seq += 1
    
    def listener(self):
        """Background thread that ONLY reads from the socket and updates the buffer."""
        while not self.closed:
            try:
                data, _ = self.socket.recvfrom() 
                    
                header = data[:self.HEADER_SIZE]
                chunk = data[self.HEADER_SIZE:]
                
                seq,flag,hash= struct.unpack("!IB16s", header)
                base_header = struct.pack("!IB", seq, flag)
                data_to_hash = base_header + chunk
                
                checksum = md5(data_to_hash).digest()
                if checksum!=hash:
                    continue
                if flag==1:
                    self.ack.add(seq)
                elif flag==0:
                    self.recv_buffer[seq] = chunk
                    ack_header = self.create_packet(seq,1)

                    self.socket.sendto(ack_header, (self.dst_ip, self.dst_port))
                elif flag==2:
                    #FIN, rec, send fin ack
                    self.fin_rec=True
                    fin_head=self.create_packet(seq,3)
                    self.socket.sendto(fin_head, (self.dst_ip, self.dst_port))
                elif flag==3:
                    #finack recv
                    self.fin_acked = True
                
            except Exception as e:
                # This exception will trigger when stoprecv() closes the socket
                if not self.closed:
                    print("listener died:", e)

    def recv(self) -> bytes:
        """Main thread blocks until the requested packet is in the buffer."""
        # 3. Wait until the listener thread populates the buffer with the sequence we need
        while self.next_seq not in self.recv_buffer:
            time.sleep(0.01) # Small sleep to prevent burning 100% CPU
            
        chunk = self.recv_buffer.pop(self.next_seq)
        self.next_seq += 1
        return chunk

        # For now, I'll just pass the full UDP payload to the app
       

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.
        fin_header = self.create_packet(self.send_seq,2)
        while not self.fin_acked:
            self.socket.sendto(fin_header, (self.dst_ip, self.dst_port))
            start_time = time.time()
            while time.time() - start_time < 0.25 and not self.fin_acked:
                time.sleep(0.01)
                
        while not self.fin_rec:
            time.sleep(0.01)

        time.sleep(0.5)
        
        self.closed = True
        self.socket.stoprecv()
        
