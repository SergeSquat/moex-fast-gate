package ru.tinkoff.moex.fast;

import org.openfast.Context;
import org.openfast.Message;
import org.openfast.codec.FastDecoder;
import org.openfast.template.MessageTemplate;
import org.openfast.template.loader.MessageTemplateLoader;
import org.openfast.template.loader.XMLMessageTemplateLoader;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.MembershipKey;
import java.util.Arrays;
import java.util.Enumeration;

/**
 * Created by s.y.terentyev on 23.03.2017.
 */
public class FASTClient {

    public static void main(String[] args) throws IOException {
        InputStream templateSource = new FileInputStream("FIX50SP2-2017-Mar.xml");
        MessageTemplateLoader templateLoader = new XMLMessageTemplateLoader();
        MessageTemplate[] templates = templateLoader.load(templateSource);
        final Context context = new Context();
        Arrays.asList(templates).stream().forEach((t) -> context.registerTemplate(Integer.parseInt(t.getId()), t));


        int port = Integer.parseInt(args[0]);


        NetworkInterface networkInterface = NetworkInterface.getByName("ppp2");
        try (DatagramChannel dc = DatagramChannel.open(StandardProtocolFamily.INET)
                .setOption(StandardSocketOptions.SO_REUSEADDR, true)
                .bind(new InetSocketAddress(port))
                .setOption(StandardSocketOptions.IP_MULTICAST_IF, networkInterface)) {
            InetAddress group = InetAddress.getByName(args[1]);
            InetAddress source = InetAddress.getByName(args[2]);
            MembershipKey key = dc.join(group, networkInterface, source);


            Thread t = new Thread(() -> {
                while (true) {
                    try {
                        while (true) {
                            ByteBuffer buf = ByteBuffer.allocate(4096);
                            dc.receive(buf);
                            //System.out.println(String.format("%d bytes received", buf.position()));
                            buf.position(4);
                            ByteBufferBackedInputStream in = new ByteBufferBackedInputStream(buf);
                            FastDecoder decoder = new FastDecoder(context, in);
                            Message message = decoder.readMessage();
                            if (message != null) {
                                System.out.println(String.format("Message template %s %s", message.getTemplate().getId(), message.getTemplate().getName()));


                                System.out.println(message.toString());
                            } else {
                                System.out.println("Null message, exiting...");
                                break;
                            }
                        }
                    } catch (IOException ex) {
                        ex.printStackTrace();
                        break;
                    }
                }
            });
            t.start();


            while (t.isAlive()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ignore) {
                    t.interrupt();
                }
            }


            key.drop();
        }
        System.exit(0);
    }

    static class ByteBufferBackedInputStream extends InputStream {

        ByteBuffer buf;

        public ByteBufferBackedInputStream(ByteBuffer buf) {
            this.buf = buf;
        }

        public int read() throws IOException {
            if (!buf.hasRemaining()) {
                return -1;
            }
            return buf.get() & 0xFF;
        }

        public int read(byte[] bytes, int off, int len)
                throws IOException {
            if (!buf.hasRemaining()) {
                return -1;
            }

            len = Math.min(len, buf.remaining());
            buf.get(bytes, off, len);
            return len;
        }
    }
}
