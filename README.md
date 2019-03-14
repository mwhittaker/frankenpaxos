# FrankenPaxos

## Getting Started
You can build and run all of frankenpaxos's code using `sbt`. All the code in
this repository can be compiled to bytecode that you can run on the JVM and can
also be compiled to javascript that you can run in the browser. As a result,
the project is split into two subprojects: `frankenpaxosJVM` for the code that
compiles to bytecode and `frankenpaxosJS` for the code that compiles to
javascript. `frankenpaxos` is a parent project of the two.

```
$ sbt
sbt:frankenpaxos> frankenpaxosJVM/compile  # Build the JVM code.
sbt:frankenpaxos> frankenpaxosJS/compile   # Build the javascript code.
sbt:frankenpaxos> frankenpaxos/compile     # Build all the code.
sbt:frankenpaxos> frankenpaxosJVM/test     # Run the tests.
sbt:frankenpaxos> frankenpaxosJS/fastOptJs # Compile to javascript.
sbt:frankenpaxos> frankenpaxosJVM/assembly # Assemble a JAR.

$ # Run the JAR.
$ java -cp jvm/target/scala-2.12/frankenpaxos-assembly-0.1.0-SNAPSHOT.jar <main>
```

Core code is in the [`shared`](shared/) directory, JVM-specific code is in the
[`jvm/`](jvm/) directory, and javascript-specific code is in the [`js/`](js/)
directory.

## Running in the Browser
The code in this repository is compiled to javascript using Scala.js. With a
bit of html and javascript, we can visualize the execution of a distributed
system in the browser. As an example, run `sbt frankenpaxosJS/fastOptJs` and
then run an HTTP server on port 8000 in the root `frankenpaxos` directory
(e.g., `python -m SimpleHTTPServer`). Open
`http://localhost:8000/js/src/main/js/election/leader_election.html` to see a
leader election protocol in action.  The source code for this example can be
found in the following files:

- [`LeaderElection.proto`](shared/src/main/scala/frankenpaxos/election/LeaderElection.proto)
- [`LeaderElection.scala`](shared/src/main/scala/frankenpaxos/election/LeaderElection.scala)
- [`LeaderElection.scala`](js/src/main/scala/frankenpaxos/election/js/LeaderElection.scala)
- [`leader_election.html`](js/src/main/js/election/leader_election.html)
- [`leader_election.js`](js/src/main/js/election/leader_election.js)

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
