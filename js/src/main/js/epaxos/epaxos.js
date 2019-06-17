let command_triple = {
  props: ['triple'],

  template: `
    <fp-object :value="triple">
      <fp-field :name="'commandOrNoop'"
                :value="triple.commandOrNoop"
                :show_at_start="false"></fp-field>
      <fp-field :name="'sequenceNumber'"
                :value="triple.sequenceNumber"></fp-field>
      <fp-field :name="'dependencies'" :value="triple.dependencies">
        <frankenpaxos-set :set="triple.dependencies">
        </frankenpaxos-set>
      </fp-field>
    </fp-object>
  `,
};

let preaccept_ok = {
  props: ['preaccept_ok'],

  template: `
    <fp-object :value="preaccept_ok">
      <fp-field :name="'instance'" :value="preaccept_ok.instance"></fp-field>
      <fp-field :name="'ballot'" :value="preaccept_ok.ballot"></fp-field>
      <fp-field :name="'replicaIndex'"
                :value="preaccept_ok.replicaIndex"></fp-field>
      <fp-field :name="'sequenceNumber'"
                :value="preaccept_ok.sequenceNumber"></fp-field>
      <fp-field :name="'dependencies'" :value="preaccept_ok.dependencies">
        <frankenpaxos-seq :seq="preaccept_ok.dependencies">
        </frankenpaxos-seq>
      </fp-field>
    </fp-object>
  `,
};

let accept_ok = {
  props: ['accept_ok'],

  template: `
    <fp-object :value="accept_ok">
      <fp-field :name="'instance'" :value="accept_ok.instance"></fp-field>
      <fp-field :name="'ballot'" :value="accept_ok.ballot"></fp-field>
      <fp-field :name="'replicaIndex'"
                :value="accept_ok.replicaIndex"></fp-field>
    </fp-object>
  `,
};

let prepare_ok = {
  props: ['prepare_ok'],

  template: `
    <fp-object :value="prepare_ok">
      <fp-field :name="'instance'" :value="prepare_ok.instance"></fp-field>
      <fp-field :name="'ballot'" :value="prepare_ok.ballot"></fp-field>
      <fp-field :name="'replicaIndex'"
                :value="prepare_ok.replicaIndex"></fp-field>
      <fp-field :name="'voteBallot'" :value="prepare_ok.voteBallot"></fp-field>
      <fp-field :name="'status'"
                :value="prepare_ok.status"></fp-field>
      <fp-field :name="'commandOrNoop'"
                :value="prepare_ok.commandOrNoop"></fp-field>
      <fp-field :name="'sequenceNumber'"
                :value="prepare_ok.sequenceNumber"></fp-field>
      <fp-field :name="'dependencies'" :value="prepare_ok.dependencies">
        <frankenpaxos-seq :seq="prepare_ok.dependencies">
        </frankenpaxos-seq>
      </fp-field>
    </fp-object>
  `,
};

let client_info = {
  props: ['node'],

  data: function() {
    return {
      proposal: "",
    };
  },

  methods: {
    propose: function() {
      if (this.proposal === "") {
        return;
      }
      this.node.actor.propose(0, this.proposal);
      this.proposal = "";
    },
  },

  template: `
    <div>
      pendingCommands =
      <frankenpaxos-map
        :map="node.actor.pendingCommands"
        v-slot="{value: pendingCommand}">
        <div v-if="pendingCommand === undefined">
          None
        </div>

        <div v-else>
          pendingCommand =
          <fp-object v-slot="{let: pc}" :value="pendingCommand">
            <fp-field :name="'id'" :value="pc.id"></fp-field>
            <fp-field :name="'command'" :value="pc.command"></fp-field>
            <fp-field :name="'result'" :value="pc.result"></fp-field>
          </fp-object>
        </div>
      </frankenpaxos-map>
      <button v-on:click="propose">Propose</button>
      <input v-model="proposal" v-on:keyup.enter="propose"></input>
    </div>
  `,
};

let replica_info = {
  props: ['node'],

  components: {
    'command-triple': command_triple,
    'preaccept-ok': preaccept_ok,
    'accept-ok': accept_ok,
    'prepare-ok': prepare_ok,
  },

  data: function() {
    return {
      options: {
        edges: {
          arrows: 'to',
        },
      },
    };
  },

  methods: {
    instance_to_string: function(instance) {
      return instance.replicaIndex + "." + instance.instanceNumber;
    },

    nodes: function() {
      let ns = this.JsUtils.setToJs(this.node.actor.dependencyGraph.nodes);
      return ns.map(instance => {
        return {
          id: this.instance_to_string(instance),
          label: this.instance_to_string(instance),
        };
      });
    },

    edges: function() {
      let es = this.JsUtils.setToJs(this.node.actor.dependencyGraph.edges);
      es = es.map(t => this.JsUtils.tupleToJs(t));
      es = es.map(t => {
        return {
          from: this.instance_to_string(t[0]),
          to: this.instance_to_string(t[1]),
        };
      });
      return es;
    },
  },

  template: `
    <div>
      <div>nextAvailableInstance = {{node.actor.nextAvailableInstance}}</div>
      <div>largestBallot = {{node.actor.largestBallot}}</div>
      <div>stateMachine = {{node.actor.stateMachine}}</div>
      <div>
        dependencyGraph =
        <frankenpaxos-graph
          style="height: 200px; border: 1pt solid black;"
          :nodes="nodes()"
          :edges="edges()"
          :options="options">
        </frankenpaxos-graph>
      </div>

      <!-- recoveryInstanceTimers -->
      <div>
        recoverInstanceTimers =
        <frankenpaxos-map :map="node.actor.recoverInstanceTimers">
        </frankenpaxos-map>
      </div>

      <!-- cmdLog -->
      <div>
        cmdLog =

        <frankenpaxos-map :map="node.actor.cmdLog" v-slot="{value: entry}">
          <!-- NoCommandEntry -->
          <div v-if="entry.constructor.name.endsWith('$NoCommandEntry')">
            <strong>NoCommandEntry</strong>
            <fp-object v-slot="{let: noCommand}" :value="entry">
              <fp-field :name="'ballot'" :value="noCommand.ballot"></fp-field>
            </fp-object>
          </div>

          <!-- PreAcceptedEntry -->
          <div v-if="entry.constructor.name.endsWith('$PreAcceptedEntry')">
            <strong>PreAcceptedEntry</strong>
            <fp-object v-slot="{let: preaccepted}" :value="entry">
              <fp-field :name="'ballot'" :value="preaccepted.ballot"></fp-field>
              <fp-field :name="'voteBallot'"
                        :value="preaccepted.voteBallot"></fp-field>
              <fp-field :name="'triple'"
                        :value="preaccepted.triple">
                <command-triple :triple="preaccepted.triple"></command-triple>
              </fp-field>
            </fp-object>
          </div>

          <!-- AcceptedEntry -->
          <div v-if="entry.constructor.name.endsWith('$AcceptedEntry')">
            <strong>AcceptedEntry</strong>
            <fp-object v-slot="{let: accepted}" :value="entry">
              <fp-field :name="'ballot'" :value="accepted.ballot"></fp-field>
              <fp-field :name="'voteBallot'"
                        :value="accepted.voteBallot"></fp-field>
              <fp-field :name="'triple'" :value="accepted.triple">
                <command-triple :triple="accepted.triple"></command-triple>
              </fp-field>
            </fp-object>
          </div>

          <!-- CommittedEntry -->
          <div v-if="entry.constructor.name.endsWith('$CommittedEntry')">
            <strong>CommittedEntry</strong>
            <fp-object v-slot="{let: committed}" :value="entry">
              <fp-field :name="'triple'" :value="committed.triple">
                <command-triple :triple="committed.triple"></command-triple>
              </fp-field>
            </fp-object>
          </div>
        </frankenpaxos-map>
      </div>

      <!-- leaderStates -->
      <div>
        leaderStates =

        <frankenpaxos-map :map="node.actor.leaderStates" v-slot="{value: state}">
          <!-- PreAccepting -->
          <div v-if="state.constructor.name.endsWith('$PreAccepting')">
            <strong>PreAccepting</strong>
            <fp-object v-slot="{let: preAccepting}" :value="state">
              <fp-field :name="'ballot'"
                        :value="preAccepting.ballot"></fp-field>
              <fp-field :name="'commandOrNoop'"
                        :value="preAccepting.commandOrNoop"
                        :show_at_start="false"></fp-field>
              <fp-field :name="'responses'"
                        :value="preAccepting.responses">
                <frankenpaxos-map :map="preAccepting.responses"
                                  v-slot="{value: preaccept_ok}">
                  <preaccept-ok :preaccept_ok="preaccept_ok"></preaccept-ok>
                </frankenpaxos-map>
              </fp-field>
              <fp-field :name="'avoidFastPath'"
                        :value="preAccepting.avoidFastPath"></fp-field>
              <fp-field :name="'resendPreAcceptsTimer'"
                        :value="preAccepting.resendPreAcceptsTimer"></fp-field>
              <fp-field :name="'defaultToSlowPathTimer'"
                        :value="preAccepting.defaultToSlowPathTimer"></fp-field>
            </fp-object>
          </div>

          <!-- Accepting -->
          <div v-if="state.constructor.name.endsWith('$Accepting')">
            <strong>Accepting</strong>
            <fp-object v-slot="{let: accepting}" :value="state">
              <fp-field :name="'ballot'" :value="accepting.ballot"></fp-field>
              <fp-field :name="'triple'" :value="accepting.triple">
                <command-triple :triple="accepting.triple"></command-triple>
              </fp-field>
              <fp-field :name="'responses'" :value="accepting.responses">
                <frankenpaxos-map :map="accepting.responses"
                                  v-slot="{value: accept_ok}">
                  <accept-ok :accept_ok="accept_ok"></accept-ok>
                </frankenpaxos-map>
              </fp-field>
              <fp-field :name="'resendAcceptsTimer'"
                        :value="accepting.resendAcceptsTimer"></fp-field>
            </fp-object>
          </div>

          <!-- Preparing -->
          <div v-if="state.constructor.name.endsWith('$Preparing')">
            <strong>Preparing</strong>
            <fp-object v-slot="{let: preparing}" :value="state">
              <fp-field :name="'ballot'" :value="preparing.ballot"></fp-field>
              <fp-field :name="'responses'" :value="preparing.responses">
                <frankenpaxos-map :map="preparing.responses"
                                  v-slot="{value: prepare_ok}">
                  <prepare-ok :prepare_ok="prepare_ok"></prepare-ok>
                </frankenpaxos-map>
              </fp-field>
              <fp-field :name="'resendPreparesTimer'"
                        :value="preparing.resendPreparesTimer"></fp-field>
            </fp-object>
          </div>
        </frankenpaxos-map>
      </div>
    </div>
  `,
};

function make_nodes(EPaxos, snap) {
  // https://flatuicolors.com/palette/defo
  let flat_red = '#e74c3c';
  let flat_blue = '#3498db';
  let flat_orange = '#f39c12';
  let flat_green = '#2ecc71';
  let colored = (color) => {
    return {
      'fill': color,
      'stroke': 'black', 'stroke-width': '3pt',
    }
  };

  let number_style = {
    'text-anchor': 'middle',
    'alignment-baseline': 'middle',
    'font-size': '20pt',
    'font-weight': 'bolder',
    'fill': 'black',
    'stroke': 'white',
    'stroke-width': '1px',
  }

  let client_x = 125;
  let replica_x = 275;

  let nodes = {};

  // Clients.
  nodes[EPaxos.client1.address] = {
    actor: EPaxos.client1,
    color: flat_red,
    svgs: [
      snap.circle(client_x, 100, 20).attr(colored(flat_red)),
      snap.text(client_x, 100, '1').attr(number_style),
    ],
    component: client_info,
  };
  nodes[EPaxos.client2.address] = {
    actor: EPaxos.client2,
    color: flat_red,
    svgs: [
      snap.circle(client_x - 100, 300, 20).attr(colored(flat_red)),
      snap.text(client_x - 100, 300, '2').attr(number_style),
    ],
    component: client_info,
  };
  nodes[EPaxos.client3.address] = {
    actor: EPaxos.client3,
    color: flat_red,
    svgs: [
      snap.circle(client_x, 500, 20).attr(colored(flat_red)),
      snap.text(client_x, 500, '3').attr(number_style),
    ],
    component: client_info,
  };

  // Replicas.
  nodes[EPaxos.replica1.address] = {
    actor: EPaxos.replica1,
    color: flat_blue,
    svgs: [
      snap.circle(replica_x, 100, 20).attr(colored(flat_blue)),
      snap.text(replica_x, 100, '1').attr(number_style),
    ],
    component: replica_info,
  };
  nodes[EPaxos.replica2.address] = {
    actor: EPaxos.replica2,
    color: flat_blue,
    svgs: [
      snap.circle(replica_x + 100, 200, 20).attr(colored(flat_blue)),
      snap.text(replica_x + 100, 200, '2').attr(number_style),
    ],
    component: replica_info,
  };
  nodes[EPaxos.replica3.address] = {
    actor: EPaxos.replica3,
    color: flat_blue,
    svgs: [
      snap.circle(replica_x - 100, 300, 20).attr(colored(flat_blue)),
      snap.text(replica_x - 100, 300, '3').attr(number_style),
    ],
    component: replica_info,
  };
  nodes[EPaxos.replica4.address] = {
    actor: EPaxos.replica4,
    color: flat_blue,
    svgs: [
      snap.circle(replica_x + 100, 400, 20).attr(colored(flat_blue)),
      snap.text(replica_x + 100, 400, '4').attr(number_style),
    ],
    component: replica_info,
  };
  nodes[EPaxos.replica5.address] = {
    actor: EPaxos.replica5,
    color: flat_blue,
    svgs: [
      snap.circle(replica_x, 500, 20).attr(colored(flat_blue)),
      snap.text(replica_x, 500, '5').attr(number_style),
    ],
    component: replica_info,
  };

  // Node titles.
  snap.text(client_x - 50, 15, 'Clients').attr({'text-anchor': 'middle'});
  snap.text(replica_x + 50, 15, 'Replicas').attr({'text-anchor': 'middle'});

  return nodes;
}

function main() {
  let EPaxos =
    frankenpaxos.epaxos.TweenedEPaxos.EPaxos;
  let snap = Snap('#animation');
  let nodes = make_nodes(EPaxos, snap);

  // Create the vue app.
  let vue_app = new Vue({
    el: '#app',

    data: {
      nodes: nodes,
      node: nodes[EPaxos.client1.address],
      transport: EPaxos.transport,
      settings: {
        time_scale: 1,
        auto_deliver_messages: true,
        auto_start_timers: true,
      },
    },

    methods: {
      send_message: function(message) {
        let src = nodes[message.src];
        let dst = nodes[message.dst];
        let src_x = src.svgs[0].attr("cx");
        let src_y = src.svgs[0].attr("cy");
        let dst_x = dst.svgs[0].attr("cx");
        let dst_y = dst.svgs[0].attr("cy");

        let svg_message = snap.circle(src_x, src_y, 9).attr({fill: '#2c3e50'});
        snap.prepend(svg_message);
        let duration = (1000 + Math.random() * 200) / 1000;
        return TweenMax.to(svg_message.node, duration, {
          attr: { cx: dst_x, cy: dst_y },
          ease: Linear.easeNone,
          onComplete: () => { svg_message.remove(); },
        });
      },

      partition: function(address) {
        nodes[address].svgs[0].attr({fill: "#7f8c8d"})
      },

      unpartition: function(address) {
        nodes[address].svgs[0].attr({fill: nodes[address].color})
      },
    },
  });

  // Select a node by clicking it.
  for (let node of Object.values(nodes)) {
    for (let svg of node.svgs) {
      svg.node.onclick = () => {
        vue_app.node = node;
      }
    }
  }
}

window.onload = main
