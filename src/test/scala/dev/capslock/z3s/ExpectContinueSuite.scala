package dev.capslock.z3s

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import java.io.{BufferedInputStream, ByteArrayOutputStream}
import java.net.{ServerSocket, Socket, URI}
import munit.CatsEffectSuite
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.model.UploadPartRequest

/** クライアントがUploadPartにExpect: 100-continueを付けないことを確かめる */
class ExpectContinueSuite extends CatsEffectSuite:
  /** 受け取ったリクエストのヘッダを記録し、200を返す */
  private def handle(socket: Socket, received: Ref[IO, List[List[String]]]): Unit =
    val in = BufferedInputStream(socket.getInputStream)
    val out = socket.getOutputStream
    def readHeaders(): List[String] =
      val buf = ByteArrayOutputStream()
      while !buf.toString.endsWith("\r\n\r\n") do
        val b = in.read()
        if b < 0 then throw Exception("eof")
        buf.write(b)
      buf.toString.split("\r\n").toList.map(_.toLowerCase)
    try
      while true do
        val headers = readHeaders()
        received.update(_ :+ headers).unsafeRunSync()(using cats.effect.unsafe.implicits.global)
        // Expectが付いていればサーバとして100 Continueを返してから本文を読む
        if headers.contains("expect: 100-continue") then
          out.write("HTTP/1.1 100 Continue\r\n\r\n".getBytes)
          out.flush()
        val length = headers.collectFirst { case h if h.startsWith("content-length:") => h.drop(15).trim.toLong }
        in.skipNBytes(length.getOrElse(0L))
        out.write("HTTP/1.1 200 OK\r\nETag: \"abc\"\r\nContent-Length: 0\r\n\r\n".getBytes)
        out.flush()
    catch case _: Exception => socket.close()

  private def server(received: Ref[IO, List[List[String]]]): Resource[IO, ServerSocket] =
    Resource.fromAutoCloseable(IO(ServerSocket(0))).flatTap { s =>
      Resource.eval(IO {
        val t = Thread(() =>
          try
            while true do
              val socket = s.accept()
              Thread(() => handle(socket, received)).start()
          catch case _: Exception => ()
        )
        t.setDaemon(true)
        t.start()
      })
    }

  test("UploadPart is sent without Expect: 100-continue") {
    System.setProperty("aws.accessKeyId", "test")
    System.setProperty("aws.secretAccessKey", "test")
    for
      received <- Ref.of[IO, List[List[String]]](Nil)
      etag <- server(received)
        .flatMap(s => S3ObjectStore.client(Some("us-east-1"), Some(URI.create(s"http://127.0.0.1:${s.getLocalPort}"))))
        .use { client =>
          val req = UploadPartRequest.builder().bucket("b").key("k").uploadId("u").partNumber(1).build()
          // SDKは1 MiB以上の本文にだけExpectを付けるため、それより大きくする
          val body = AsyncRequestBody.fromBytes(Array.fill[Byte](5 * 1024 * 1024)(1))
          IO.fromCompletableFuture(IO(client.uploadPart(req, body))).map(_.eTag())
        }
      requests <- received.get
    yield
      assertEquals(etag, "\"abc\"")
      assertEquals(requests.size, 1)
      assert(!requests.head.exists(_.startsWith("expect:")), requests.head.mkString("\n"))
  }
