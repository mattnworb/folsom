/*
 * Copyright (c) 2015 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.spotify.folsom;

import com.google.common.base.Charsets;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class MisbehavingServerTest {

  private static final Logger log = LoggerFactory.getLogger(MisbehavingServerTest.class);

  private Server server;

  @Before
  public void setup() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
    server.stop();
  }

  @Test
  public void testInvalidAsciiResponse() throws Throwable {
    testAsciiGet("HIPPO\r\n", "Unexpected line: HIPPO");
  }

  @Test
  public void testInvalidAsciiResponse2() throws Throwable {
    testAsciiGet("HIPPOS\r\n", "Unexpected line: HIPPOS");
  }

  @Test
  public void testInvalidAsciiResponse3() throws Throwable {
    testAsciiGet("AAAAAAAAAAAAAAARGH\r\n", "Unexpected line: AAAAAAAAAAAAAAARGH");
  }

  @Test
  public void testAsciiNotANumber() throws Throwable {
    testAsciiGet("123ABC\r\n", "Unexpected line: 123ABC");
  }

  @Test
  public void testEmptyAsciiResponse() throws Throwable {
    testAsciiGet("\r\n", "Unexpected line: ");
  }

  @Test
  public void testNotNewline() throws Throwable {
    testAsciiGet("\rFoo\n", "Expected newline, got something else");
  }

  @Test
  public void testBadAsciiGet() throws Throwable {
    testAsciiGet("VALUE\r\n", "Unexpected line: VALUE");
  }

  @Test
  public void testBadAsciiGet2() throws Throwable {
    testAsciiGet("VALUE \r\n", "Unexpected line: VALUE ");
  }

  @Test
  public void testBadAsciiGet3() throws Throwable {
    testAsciiGet("VALUE key\r\n", "Unexpected line: VALUE key");
  }

  @Test
  public void testBadAsciiGet4() throws Throwable {
    testAsciiGet("VALUE key 123\r\n", "Unexpected line: VALUE key 123");
  }

  @Test
  public void testBadAsciiGet5() throws Throwable {
    testAsciiGet("VALUE key 123 456\r\n", "Timeout");
  }

  @Test
  public void testBadAsciiGet6() throws Throwable {
    testAsciiGet("VALUE key 123 0\r\nfoo\r\n", "Unexpected end of data block: foo");
  }

  @Test
  public void testBadAsciiGet7() throws Throwable {
    testAsciiGet("VALUE key 123 0\r\n\r\nSTORED\r\n", "Unexpected line: STORED");
  }

  @Test
  public void testBadAsciiGet8() throws Throwable {
    testAsciiGet("VALUE key 123 1a3\r\n", "Unexpected line: VALUE key 123 1a3");
  }

  @Test
  public void testWrongAsciiKey() throws Throwable {
    testAsciiGet("VALUE otherkey 123 0\r\n\r\nEND\r\n", "Expected key key but got otherkey");
  }

  @Test
  public void testTooManyAsciiValues() throws Throwable {
    testAsciiGet("" +
            "VALUE key 123 0\r\n" +
            "\r\n" +
            "VALUE key 123 0\r\n" +
            "\r\n" +
            "END\r\n",
            "Too many responses, expected 1 but got 2");
  }

  @Test
  public void testAsciiWrongResponseType() throws Throwable {
    testAsciiGet("1234\r\n", "Unexpected response type: NUMERIC_VALUE");
  }

  @Test
  public void testBadAsciiTouch() throws Throwable {
    testAsciiTouch("STORED\r\n", "Unexpected line: STORED");
  }

  @Test
  public void testBadAsciiSet() throws Throwable {
    testAsciiSet("TOUCHED\r\n", "Unexpected line: TOUCHED");
  }

  private void testAsciiGet(String response, String expectedError) throws Exception {
    MemcacheClient<String> client = setupAscii(response);
    try {
      client.get("key").get();
      fail();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      assertEquals(MemcacheClosedException.class, cause.getClass());
      assertEquals(expectedError, cause.getMessage());
    }
  }

  private void testAsciiTouch(String response, String expectedError) throws Exception {
    MemcacheClient<String> client = setupAscii(response);
    try {
      client.touch("key", 123).get();
      fail();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      assertEquals(MemcacheClosedException.class, cause.getClass());
      assertEquals(expectedError, cause.getMessage());
    }
  }

  private void testAsciiSet(String response, String expectedError) throws Exception {
    MemcacheClient<String> client = setupAscii(response);
    try {
      client.set("key", "value", 123).get();
      fail();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      assertEquals(MemcacheClosedException.class, cause.getClass());
      assertEquals(expectedError, cause.getMessage());
    }
  }

  private MemcacheClient<String> setupAscii(String response) throws Exception {
    server = new Server(response);
    return createClient(server, 100L, 100);
  }

  private MemcacheClient<String> createClient(Server server, long timeoutMillis, int maxOutstanding) throws Exception {
    MemcacheClient<String> client = MemcacheClientBuilder.newStringClient()
            .withAddress(HostAndPort.fromParts("localhost", server.port))
            .withMaxOutstandingRequests(maxOutstanding)
            .withRequestTimeoutMillis(timeoutMillis)
            .withRetry(false)
            .connectAscii();
    ConnectFuture.connectFuture(client).get();
    return client;
  }

  @Test
  public void testServerGoesAway() throws Throwable {
    server = new Server("VALUE key 0 2\r\nOK\r\nEND\r\n");

    final int maxOutstanding = 100;
    MemcacheClient<String> client = createClient(server, 1000L, maxOutstanding);

    // test Ok
    assertEquals("OK", client.get("key").get());

    // have the server stop sending responses
    server.setActive(false);

    // send enough additional requests to fill up the outstanding queue
    for (int i = 0; i < maxOutstanding; i++) {
      ListenableFuture<String> future = client.get("key");
      assertFalse(future.isDone());
    }

    // re-enable the server, and pause to let the outstanding requests all "expire"
    server.setActive(true);
    Thread.sleep(1500);

    assertEquals(1, client.numActiveConnections());

    // send a bunch more requests, expecting them to succeed since the previous requests should have timed out
    int successes = 0;
    int tooManyOutstanding = 0;
    int otherFailures = 0;
    List<ListenableFuture<String>> futures = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      futures.add(client.get("key"));
    }

    // sleep some more to give the original outstanding requests another chance to expire
    Thread.sleep(3000);
    for (int i = 0; i < 10; i++) {
      futures.add(client.get("key"));
    }

    for (ListenableFuture<String> f : futures) {
      try {
        f.get();
        successes++;
      } catch (ExecutionException e) {
        if (e.getCause() instanceof MemcacheOverloadedException) {
          tooManyOutstanding++;
        } else {
          otherFailures++;
        }
      }
    }

    log.info("successes={}, tooManyOutstanding={}, otherFailures={}", successes, tooManyOutstanding, otherFailures);
    assertEquals(0, tooManyOutstanding);
    assertEquals(20, successes);
  }

  private static class Server {
    private final int port;
    private final ServerSocket serverSocket;
    private final Thread thread;

    private volatile Throwable failure;
    private volatile Socket socket;
    private volatile boolean active = true;

    private Server(String responseString) throws IOException {
      final byte[] response = responseString.getBytes(Charsets.UTF_8);
      serverSocket = new ServerSocket(0);
      port = serverSocket.getLocalPort();
      thread = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            socket = serverSocket.accept();
            if (active) {
              handleConnection(socket);
            }
          } catch (Throwable e) {
            failure = e;
            failure.printStackTrace();
          }
        }

        private void handleConnection(Socket socket) throws Exception {
          BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()), 1);
          String s = reader.readLine();
          if (s.startsWith("get ") || s.startsWith("touch ")) {
            // Don't need to read any more lines
          } else if (s.startsWith("set ")) {
            // Read the value too
            reader.readLine();
          } else {
            throw new RuntimeException("Unhandled command: " + s);
          }
          socket.getOutputStream().write(response);
          socket.getOutputStream().flush();
        }
      });
      thread.start();
    }

    public void setActive(boolean active) {
      this.active = active;
    }

    public void stop() throws Exception {
      thread.join();
      serverSocket.close();
      if (socket != null) {
        socket.close();
      }
      if (failure != null) {
        fail(failure.getClass().getSimpleName() + ": " + failure.getMessage());
      }
    }
  }
}
