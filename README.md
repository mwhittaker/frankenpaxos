# Zeno
Zeno is a library for writing distributed systems in scala. Distributed systems
written with zeno can be run as plain Jane scala programs, or they can be
compiled to javascript (using [Scala.js](https://www.scala-js.org/)) and run in
the browser.

## Getting Started
You can build and run all of zeno's code using `sbt`. Distributed systems
written with zeno can be compiled to bytecode that you can run on the JVM and
can be compiled to javascript that you can run in the browser. As a result, the
project is split into two projects: `zenoJVM` for the code that compiles to
bytecode and `zenoJS` for the code that compiles to javascript. `zeno` is a
parent project of the two.

```
$ sbt
sbt:zeno> zenoJVM/compile  # Build the JVM code.
sbt:zeno> zenoJS/compile   # Build the javascript code.
sbt:zeno> zeno/compile     # Build all the code.
sbt:zeno> zenoJVM/test     # Run the tests.
sbt:zeno> zenoJS/fastOptJs # Compile to javascript.
```

Core code is in the [`shared`](shared/) directory, JVM-specific code is in the
[`jvm/`](jvm/) directory, and javascript-specific code is in the [`js/`](js/)
directory.

## Running in the Browser
We can implement a distributed system with zeno and then compile it to
javascript using Scala.js. Then, with a bit of html and javascript, we can
visualize the execution of the distributed system in the browser. As an
example, run `sbt zenoJS/fastOptJs` and then run an HTTP server on port 8000 in
the root `zeno` directory (e.g., `python -m SimpleHTTPServer`). Open
`http://localhost:8000/js/src/main/js/examples/echo.html` to see two echo
clients communicate with an echo server. The source code for this example can
be found in the following files:

- [`Echo.proto`](shared/src/main/scala/zeno/examples/Echo.proto)
- [`EchoClient.scala`](shared/src/main/scala/zeno/examples/EchoClient.scala)
- [`EchoServer.scala`](shared/src/main/scala/zeno/examples/EchoServer.scala)
- [`Echo.scala`](js/src/main/scala/zeno/examples/js/Echo.scala)
- [`echo.html`](js/src/main/js/examples/echo.html)
- [`echo.js`](js/src/main/js/examples/echo.js)
