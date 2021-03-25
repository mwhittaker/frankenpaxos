# FrankenPaxos

## Getting Started
Make sure you have Java, Scala, and sbt installed. We tested everything with
Java version 1.8.0_131 and with sbt version 1.1.6 on Ubuntu 18.04. You can
install these however you like, but one way is to use [this
script](https://raw.githubusercontent.com/mwhittaker/vms/master/install_java8.sh)
and [this
script](https://raw.githubusercontent.com/mwhittaker/vms/master/install_scala.sh).

You can build and run all of frankenpaxos' code using `sbt`. All the code in
this repository can be compiled to bytecode that you can run on the JVM and can
also be compiled to Javascript that you can run in the browser. As a result,
the project is split into two subprojects: `frankenpaxosJVM` for the code that
compiles to bytecode and `frankenpaxosJS` for the code that compiles to
Javascript. `frankenpaxos` is a parent project of the two.

```bash
$ sbt
sbt:frankenpaxos> frankenpaxosJVM/compile  # Build the JVM code.
sbt:frankenpaxos> frankenpaxosJS/compile   # Build the Javascript code.
sbt:frankenpaxos> frankenpaxos/compile     # Build all the code.
sbt:frankenpaxos> frankenpaxosJVM/test     # Run the tests.
sbt:frankenpaxos> frankenpaxosJS/fastOptJS # Compile to Javascript.
sbt:frankenpaxos> frankenpaxosJVM/assembly # Assemble a JAR.

$ # Run the JAR. Replace <main> with one of the executable classes. You can use
$ # tab completion to see the full list of possibilities.
$ java -cp jvm/target/scala-2.12/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar <main>
$
$ # For example, we can run the echo server.
$ java -cp jvm/target/scala-2.12/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar \
>     frankenpaxos.echo.ServerMain --help
```

Core code is in the [`shared`](shared/) directory, JVM-specific code is in the
[`jvm/`](jvm/) directory, and Javascript-specific code is in the [`js/`](js/)
directory.

## Running in the Browser
The code in this repository is compiled to Javascript using Scala.js. With a
bit of HTML and Javascript, we can visualize the execution of a distributed
system in the browser. For example, visit
https://mwhittaker.github.io/frankenpaxos/js/src/main/js/echo/echo.html for a
visualization of a simple echo protocol.

To run the visualization locally, run `sbt frankenpaxosJS/fastOptJs` and then
run an HTTP server on port 8000 in the root `frankenpaxos` directory (e.g., by
running `python -m http.server 8000`). Open
`http://localhost:8000/js/src/main/js/echo/echo.html`.  The source code for
this example can be found in the following files:

- [`Echo.proto`](shared/src/main/scala/frankenpaxos/echo/Echo.proto)
- [`Server.scala`](shared/src/main/scala/frankenpaxos/echo/Server.scala)
- [`Client.scala`](shared/src/main/scala/frankenpaxos/echo/Client.scala)
- [`Echo.scala`](js/src/main/scala/frankenpaxos/echo/Echo.scala)
- [`echo.html`](js/src/main/js/echo/echo.html)
- [`echo.js`](js/src/main/js/echo/echo.js)

## Using Eclim
[Eclim](http://eclim.org/eclimd.html) is a vim plugin that lets you use all the
features and functionality of Eclipse from within vim. To use eclim with this
project, first install eclim and all necessary dependencies. Then, do the
following:

1. Run `eclipse` from within `sbt`. This will create a `.project` file and a
   `.classpath` file in the `jvm/` directory.
2. Copy `jvm/.project` and `jvm/.classpath` into the root directory.
3. Remove the `linkedResources` section in `.project`. It is not needed.
4. Remove the `kind="src"` class path entries from the top of `.classpath`.
   Replace them with the following:

   ```
   <classpathentry kind="src" path="shared/src/main/scala"/>
   <classpathentry kind="src" path="shared/src/test/scala"/>
   <classpathentry kind="src" path="js/src/main/scala"/>
   <classpathentry kind="src" path="jvm/src/main/scala"/>
   <classpathentry kind="src" path="jvm/target/scala-2.12/src_managed/main"/>
   ```
5. Open up vim and run `:ProjectCreate . -n scala` or `:ProjectRefresh` if the
   project already exists.

## Running Benchmarks
See the `benchmarks/` directory for information on running benchmarks.

## Updating Github Pages
```bash
git checkout master
git branch -f gh-pages master
git checkout gh-pages
git add -f js/target/scala-2.12/frankenpaxos-fastopt.js*
git commit
git push -f origin gh-pages
```
