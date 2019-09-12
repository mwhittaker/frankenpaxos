// Helper components ///////////////////////////////////////////////////////////
const command_component = {
  props: {
    value: Object,
  },

  template: `
    <fp-object>
      <fp-field
        :name="'clientAddress'"
        :value="value.clientAddress">
      </fp-field>
      <fp-field
        :name="'clientPseudonym'"
        :value="value.clientPseudonym">
      </fp-field>
      <fp-field
        :name="'clientId'"
        :value="value.clientId">
      </fp-field>
      <fp-field
        :name="'command'"
        :value="value.command">
      </fp-field>
    </fp-object>
  `,
};

const snapshot_component = {
  props: {
    value: Object,
  },

  template: `
    <fp-object>
      <fp-field
        :name="'id'"
        :value="value.id">
      </fp-field>
      <fp-field
        :name="'watermark'"
        :value="value.watermark">
      </fp-field>
      <fp-field
        :name="'stateMachine'"
        :value="value.stateMachine">
      </fp-field>
      <fp-field
        :name="'clientTable'"
        :value="value.clientTable">
      </fp-field>
    </fp-object>
  `,
};

const dependency_reply_component = {
  props: {
    value: Object,
  },

  template: `
    <fp-object>
      <fp-field
        :name="'vertexId'"
        :value="value.vertexId">
      </fp-field>
      <fp-field
        :name="'depServiceNodeIndex'"
        :value="value.depServiceNodeIndex">
      </fp-field>
      <fp-field :name="'dependency'">
        <frankenpaxos-seq :seq="value.dependency"></frankenpaxos-seq>
      </fp-field>
    </fp-object>
  `,
};

const command_or_snapshot = {
  props: {
    value: Object,
  },

  components: {
    'command': command_component,
  },

  // TODO(mwhittaker): Try and format this nicely. It's not easy to do since
  // value.value is of a type that is not JSExported.
  template: `
    <div>
      {{value}}
    </div>
  `,
};

const proposal_component = {
  props: {
    value: Object,
  },

  components: {
    'command': command_component,
    'snapshot': snapshot_component,
  },

  // TODO(mwhittaker): Try and format this nicely. It's not easy to do since
  // value.value is of a type that is not JSExported.
  template: `
    <div>
      <div v-if="value.value.constructor.name.endsWith('Command')">
        <command :value="value.getCommand"></command>
      </div>
      <div v-if="value.value.constructor.name.endsWith('Noop')">
        Noop
      </div>
      <div v-if="value.value.constructor.name.endsWith('Snapshot')">
        <snapshot :value="value.getSnapshot"></snapshot>
      </div>
    </div>
  `,
};

const vote_value_proto_component = {
  props: {
    value: Object,
  },

  components: {
    'proposal': proposal_component,
  },

  template: `
    <fp-object>
      <fp-field :name="'proposal'">
        <proposal :value="value.proposal"></proposal>
      </fp-field>
      <fp-field :name="'dependency'" :value="value.dependencies">
      </fp-field>
    </fp-object>
  `,
};

const phase1b_component = {
  props: {
    value: Object,
  },

  components: {
    'vote-value-proto': vote_value_proto_component,
  },

  template: `
    <fp-object>
      <fp-field :name="'vertexId'" :value="'value.vertexId'"></fp-field>
      <fp-field :name="'acceptorId'" :value="'value.acceptorId'"></fp-field>
      <fp-field :name="'round'" :value="'value.round'"></fp-field>
      <fp-field :name="'voteRound'" :value="'value.voteRound'"></fp-field>
      <fp-field :name="'voteValue'">
        <vote-value-proto :value="value.voteValue">
        </vote-value-proto>
      </fp-field>
    </fp-object>
  `,
};

const phase2b_component = {
  props: {
    value: Object,
  },

  template: `
    <fp-object>
      <fp-field :name="'vertexId'" :value="'value.vertexId'"></fp-field>
      <fp-field :name="'acceptorId'" :value="'value.acceptorId'"></fp-field>
      <fp-field :name="'round'" :value="'value.round'"></fp-field>
    </fp-object>
  `,
};

// Node components /////////////////////////////////////////////////////////////
const client_info = {
  props: {
    node: Object,
  },

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
      <div>
        ids = <frankenpaxos-map :map="node.actor.ids"></frankenpaxos-map>
      </div>

      <div>
        pendingCommands =
        <frankenpaxos-map
          :map="node.actor.pendingCommands"
          v-slot="{value: pc}">
          <fp-object :value="pc">
            <fp-field :name="'pseudonym'" :value="pc.pseudonym"></fp-field>
            <fp-field :name="'id'" :value="pc.id"></fp-field>
            <fp-field :name="'command'" :value="pc.command"></fp-field>
            <fp-field :name="'result'" :value="pc.result"></fp-field>
          </fp-object>
        </frankenpaxos-map>
      </div>

      <button v-on:click="propose">Propose</button>
      <input v-model="proposal" v-on:keyup.enter="propose"></input>
    </div>
  `,
}

let leader_info = {
  props: {
    node: Object,
  },

  components: {
    'command': command_component,
    'command-or-snapshot': command_or_snapshot,
    'dependency-reply': dependency_reply_component,
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

  template: `
    <div>
      <div>nextVertexId = {{node.actor.nextVertexId}}</div>

      <div>
        states =
        <frankenpaxos-map :map="node.actor.states" v-slot="{value: state}">
          <fp-object>
            <fp-field :name="'commandOrSnapshot'">
              <command-or-snapshot :value="state.commandOrSnapshot">
              </command-or-snapshot>
            </fp-field>
            <fp-field :name="'dependencyReplies'">
              <frankenpaxos-map
                :map="state.dependencyReplies"
                v-slot="{value: reply}">
                <dependency-reply :value="reply"></dependency-reply>
              </frankenpaxos-map>
            </fp-field>
            <fp-field
              :name="'resendDependencyRequestsTimer'"
              :value="state.resendDependencyRequestsTimer">
            </fp-field>
          </fp-object>
        </frankenpaxos-map>
      </div>
    </div>
  `,
};;

const proposer_info = {
  props: {
    node: Object,
  },

  components: {
    'proposal': proposal_component,
    'vote-value-proto': vote_value_proto_component,
    'phase1b': phase1b_component,
    'phase2b': phase2b_component,
  },

  template: `
    <div>
      <div>
        states =
        <frankenpaxos-map :map="node.actor.states" v-slot="{value: state}">
          <div v-if="state.constructor.name.endsWith('Phase1')">
            Phase1
            <fp-object>
              <fp-field :name="'round'" :value="state.round"></fp-field>
              <fp-field :name="'value'">
                <vote-value-proto :value="state.value"></vote-value-proto>
              </fp-field>
              <fp-field :name="'phase1bs'">
                <frankenpaxos-map :map="state.phase1bs"
                                  v-slot="{value: phase1b}">
                  <phase1b :value="phase1b"></phase1b>
                </frankenpaxos-map>
              </fp-field>
              <fp-field
                :name="'resendPhase1as'"
                :value="state.resendPhase1as">
              </fp-field>
            </fp-object>
          </div>

          <div v-if="state.constructor.name.endsWith('Phase2')">
            Phase2
            <fp-object v-if="state.constructor.name.endsWith('Phase2')">
              <fp-field :name="'round'" :value="state.round"></fp-field>
              <fp-field :name="'value'">
                <vote-value-proto :value="state.value"></vote-value-proto>
              </fp-field>
              <fp-field :name="'phase2bs'">
                <frankenpaxos-map :map="state.phase2bs"
                                  v-slot="{value: phase2b}">
                  <phase2b :value="phase2b"></phase2b>
                </frankenpaxos-map>
              </fp-field>
              <fp-field
                :name="'resendPhase2as'"
                :value="state.resendPhase2as">
              </fp-field>
            </fp-object>
          </div>

          <div v-if="state.constructor.name.endsWith('Chosen')">
            Chosen
            <fp-object>
              <fp-field :name="'proposal'">
                <proposal :value="state.proposal"></proposal>
              </fp-field>
              <fp-field :name="'dependencies'" :value="state.dependencies">
              </fp-field>
            </fp-object>
          </div>
        </frankenpaxos-map>
      </div>

      <div>
        gcQuorumWatermarkVector = {{node.actor.gcQuorumWatermarkVector}}
      </div>

      <div>
        gcWatermark = {{node.actor.gcWatermark}}
      </div>
    </div>
  `,
};

let dep_node_info = {
  props: {
    node: Object,
  },

  methods: {
    conflictIndex: function() {
      return this.node.actor.conflictIndex;
    },
  },

  // TODO(mwhittaker): Improve display of conflictIndex.
  template: `
    <div>
      <div v-if="conflictIndex().constructor.name.endsWith('Uncompacted')">
        <div>
          conflictIndex =
          <fp-object>
            <fp-field :name="'topOne'"
                      v-if="node.actor.options.topKDependencies == 1">
              <frankenpaxos-top-one
                :value="conflictIndex().conflictIndex.topOne">
              </frankenpaxos-top-one>
            </fp-field>
            <fp-field :name="'topK'" v-else>
              <frankenpaxos-top-k
                :value="conflictIndex().conflictIndex.topK">
              </frankenpaxos-top-k>
            </fp-field>
          </fp-object>
        </div>
        <div>
          highWatermark =
          {{conflictIndex().highWatermark}}
        </div>
      </div>

      <div v-if="conflictIndex().constructor.name.endsWith('Compacted')">
        <div>
          conflictIndex =
          <fp-object>
            <fp-field :name="'newConflictIndex'">
              <fp-object>
                <fp-field :name="'commandsAndSnapshots'">
                  {{conflictIndex().conflictIndex
                                   .newConflictIndex
                                   .commandsAndSnapshots}}
                </fp-field>
              </fp-object>
            </fp-field>
            <fp-field
              :name="'newWatermark'"
              :value="conflictIndex().conflictIndex.newWatermark">
            </fp-field>
            <fp-field :name="'oldConflictIndex'">
              <fp-object>
                <fp-field :name="'commandsAndSnapshots'">
                  {{conflictIndex().conflictIndex
                                   .oldConflictIndex
                                   .commandsAndSnapshots}}
                </fp-field>
              </fp-object>
            </fp-field>
            <fp-field
              :name="'oldWatermark'"
              :value="conflictIndex().conflictIndex.oldWatermark">
            </fp-field>
            <fp-field
              :name="'gcWatermark'"
              :value="conflictIndex().conflictIndex.gcWatermark">
            </fp-field>
          </fp-object>
        </div>
      </div>
    </div>
  `,
};

let acceptor_info = {
  props: {
    node: Object,
  },

  components: {
    'vote-value-proto': vote_value_proto_component,
  },

  template: `
    <div>
      <div>
        <strong>states</strong> =

        <frankenpaxos-foreach
            :value="node.actor.states.bufferMaps"
            v-slot="{value: value}">
          <frankenpaxos-buffer-map :value="value" v-slot="{value: state}">
            <frankenpaxos-option :value="state">
              <template v-slot:if="{value: state}">
                <fp-object>
                  <fp-field :name="'round'" :value="state.round">
                  </fp-field>
                  <fp-field :name="'voteRound'" :value="state.voteRound">
                  </fp-field>
                  <fp-field :name="'voteValue'">
                    <frankenpaxos-option :value="state.voteValue">
                      <template v-slot:if="{value: voteValue}">
                        <vote-value-proto :value="voteValue"></vote-value-proto>
                      </template>
                      <template v-slot:else>
                        None
                      </template>
                    </frankenpaxos-option>
                  </fp-field>
                </fp-object>
              </template>

              <template v-slot:else>
                None
              </template>
            </frankenpaxos-option>
          </frankenpaxos-buffer-map>
        </frankenpaxos-foreach>
      </div>

      <div>
        <strong>gcQuorumWatermarkVector</strong> =
        {{node.actor.gcQuorumWatermarkVector}}
      </div>

      <div>
        <strong>gcWatermark</strong> = {{node.actor.gcWatermark}}
      </div>
    </div>
  `,
};

let replica_info = {
  props: {
    node: Object,
  },

  components: {
    'proposal': proposal_component,
  },

  template: `
    <div>
      <div>stateMachine = {{node.actor.stateMachine}}</div>
      <div>
        numCommandsPendingWatermark =
        {{node.actor.numCommandsPendingWatermark}}
      </div>
      <div>
        numCommandsPendingSendSnapshot =
        {{node.actor.numCommandsPendingSendSnapshot}}
      </div>
      <div>
        numCommandsPendingExecution =
        {{node.actor.numCommandsPendingExecution}}
      </div>

      <div>
        clientTable =
        <frankenpaxos-client-table :clientTable="node.actor.clientTable">
        </frankenpaxos-client-table>
      </div>

      <div>
        commands =
        <frankenpaxos-foreach
            :value="node.actor.commands.bufferMaps"
            v-slot="{value: value, index: index}">
          Leader {{index}}:
          <frankenpaxos-buffer-map :value="value" v-slot="{value: committed}">
            <frankenpaxos-option :value="committed">
              <template v-slot:if="{value: committed}">
                <fp-object>
                  <fp-field :name="'proposal'">
                    <proposal :value="committed.proposal"></proposal>
                  </fp-field>
                  <fp-field :name="'dependencies'">
                    {{committed.dependencies}}
                  </fp-field>
                </fp-object>
              </template v-slot:if="{value: value}">

              <template v-slot:else="_">
                None
              </template>
            </frankenpaxos-option>
          </frankenpaxos-buffer-map>
        </frankenpaxos-foreach>
      </div>

      <div>
        committedVertices = {{node.actor.committedVertices}}
      </div>

      <div>
        executedVertices = {{node.actor.executedVertices}}
      </div>

      <div>
        snapshot = {{node.actor.snapshot}}
      </div>

      <div>
        history =
        <frankenpaxos-horizontal-seq :seq="node.actor.history">
        </frankenpaxos-horizontal-seq>
      </div>

      <div>
        dependencyGraph =
        <frankenpaxos-tarjan :value="node.actor.dependencyGraph">
        </frankenpaxos-tarjan>
      </div>
    </div>
  `,
};

let garbage_collector_info = {
  props: {
    node: Object,
  },

  template: `<div></div>`,
};

// Main app ////////////////////////////////////////////////////////////////////
function make_nodes(SimpleGcBPaxos, snap) {
  // https://flatuicolors.com/palette/defo
  let flat_red = '#e74c3c';
  let flat_blue = '#3498db';
  let flat_orange = '#f39c12';
  let flat_green = '#2ecc71';
  let flat_purple = '#9b59b6';
  let flat_dark_blue = '#2c3e50';
  let flat_dark_blue_alt = '#34495e';

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

  const client_x = 100;
  const leader_x = 300;
  const proposer_x = 335;
  const dep_service_x = 500;
  const acceptor_x = 500;
  const replica_y = 750;
  const garbage_collector_y = 800;

  let nodes = {};

  // Clients.
  const clients = [
    {client: SimpleGcBPaxos.client1, y: 200},
    {client: SimpleGcBPaxos.client2, y: 400},
    {client: SimpleGcBPaxos.client3, y: 600},
  ]
  for (const [index, {client, y}] of clients.entries()) {
    nodes[client.address] = {
      actor: client,
      color: flat_red,
      component: client_info,
      svgs: [
        snap.circle(client_x, y, 20).attr(colored(flat_red)),
        snap.text(client_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Leaders and proposers.
  const leaders_and_proposers = [
    {leader: SimpleGcBPaxos.leader1, proposer: SimpleGcBPaxos.proposer1, y: 200},
    {leader: SimpleGcBPaxos.leader2, proposer: SimpleGcBPaxos.proposer2, y: 300},
    {leader: SimpleGcBPaxos.leader3, proposer: SimpleGcBPaxos.proposer3, y: 400},
    {leader: SimpleGcBPaxos.leader4, proposer: SimpleGcBPaxos.proposer4, y: 500},
    {leader: SimpleGcBPaxos.leader5, proposer: SimpleGcBPaxos.proposer5, y: 600},
  ]
  for (const [index, {leader, proposer, y}] of leaders_and_proposers.entries()) {
    nodes[leader.address] = {
      actor: leader,
      color: flat_blue,
      component: leader_info,
      svgs: [
        snap.circle(leader_x, y, 20).attr(colored(flat_blue)),
        snap.text(leader_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
    nodes[proposer.address] = {
      actor: proposer,
      color: flat_green,
      component: proposer_info,
      svgs: [
        snap.circle(proposer_x, y + 35, 20).attr(colored(flat_green)),
        snap.text(proposer_x, y + 35, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Dependency service nodes.
  const dep_nodes = [
    {dep_node: SimpleGcBPaxos.depServiceNode1, y: 100},
    {dep_node: SimpleGcBPaxos.depServiceNode2, y: 200},
    {dep_node: SimpleGcBPaxos.depServiceNode3, y: 300},
  ]
  for (const [index, {dep_node, y}] of dep_nodes.entries()) {
    nodes[dep_node.address] = {
      actor: dep_node,
      color: flat_purple,
      component: dep_node_info,
      svgs: [
        snap.circle(dep_service_x, y, 20).attr(colored(flat_purple)),
        snap.text(dep_service_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Acceptors.
  const acceptors = [
    {acceptor: SimpleGcBPaxos.acceptor1, y: 500},
    {acceptor: SimpleGcBPaxos.acceptor2, y: 600},
    {acceptor: SimpleGcBPaxos.acceptor3, y: 700},
  ]
  for (const [index, {acceptor, y}] of acceptors.entries()) {
    nodes[acceptor.address] = {
      actor: acceptor,
      color: flat_orange,
      component: acceptor_info,
      svgs: [
        snap.circle(acceptor_x, y, 20).attr(colored(flat_orange)),
        snap.text(acceptor_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Replicas.
  const replicas = [
    {replica: SimpleGcBPaxos.replica1, x: 175},
    {replica: SimpleGcBPaxos.replica2, x: 375},
  ]
  for (const [index, {replica, x}] of replicas.entries()) {
    nodes[replica.address] = {
      actor: replica,
      color: flat_dark_blue,
      component: replica_info,
      svgs: [
        snap.circle(x, replica_y, 20).attr(colored(flat_dark_blue)),
        snap.text(x, replica_y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Garbage collectors
  const collectors = [
    {garbage_collector: SimpleGcBPaxos.garbageCollector1, x: 225},
    {garbage_collector: SimpleGcBPaxos.garbageCollector2, x: 425},
  ]
  for (const [index, {garbage_collector, x}] of collectors.entries()) {
    nodes[garbage_collector.address] = {
      actor: garbage_collector,
      color: flat_dark_blue_alt,
      component: garbage_collector_info,
      svgs: [
        snap.circle(x, garbage_collector_y, 20)
            .attr(colored(flat_dark_blue)),
        snap.text(x, garbage_collector_y, (index + 1).toString())
            .attr(number_style),
      ],
    };
  }

  // Node titles.
  snap.text(100, 50, 'Clients').attr({'text-anchor': 'middle'});
  snap.text(300, 50, 'Leaders').attr({'text-anchor': 'middle'});
  snap.text(500, 40, 'Dep Service /').attr({'text-anchor': 'middle'});
  snap.text(500, 60, 'Acceptors').attr({'text-anchor': 'middle'});
  snap.text(300, 850, 'Replicas').attr({'text-anchor': 'middle'});

  return nodes;
}

function main() {
  const SimpleGcBPaxos = frankenpaxos.simplegcbpaxos.SimpleGcBPaxos.SimpleGcBPaxos;
  const snap = Snap('#animation');
  const nodes = make_nodes(SimpleGcBPaxos, snap);

  // Create the vue app.
  let vue_app = new Vue({
    el: '#app',

    data: {
      nodes: nodes,
      node: nodes[SimpleGcBPaxos.client1.address],
      transport: SimpleGcBPaxos.transport,
      settings: {
        time_scale: 1,
        auto_deliver_messages: true,
        auto_start_timers: true,
      },
    },

    methods: {
      distance: function(x1, y1, x2, y2) {
        const dx = x1 - x2;
        const dy = y1 - y2;
        return Math.sqrt(dx*dx + dy*dy);
      },

      send_message: function(message) {
        let src = nodes[message.src];
        let dst = nodes[message.dst];
        let src_x = src.svgs[0].attr("cx");
        let src_y = src.svgs[0].attr("cy");
        let dst_x = dst.svgs[0].attr("cx");
        let dst_y = dst.svgs[0].attr("cy");
        let d = this.distance(src_x, src_y, dst_x, dst_y);
        let speed = 400 + (Math.random() * 50); // px per second.

        let svg_message = snap.circle(src_x, src_y, 9).attr({fill: '#2c3e50'});
        snap.prepend(svg_message);
        let duration = d / speed;
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
