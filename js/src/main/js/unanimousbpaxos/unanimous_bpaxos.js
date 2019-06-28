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

const command_or_noop_component = {
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

const vote_value_component = {
  props: {
    value: Object,
  },

  components: {
    'command-or-noop': command_or_noop_component,
  },

  template: `
    <fp-object>
      <fp-field :name="'commandOrNoop'">
        <command-or-noop :value="value.commandOrNoop">
        </command-or-noop>
      </fp-field>
      <fp-field :name="'dependencies'">
        <frankenpaxos-set :set="value.dependencies">
        </frankenpaxos-set>
      </fp-field>
    </fp-object>
  `,
};

const vote_value_proto_component = {
  props: {
    value: Object,
  },

  components: {
    'command-or-noop': command_or_noop_component,
  },

  template: `
    <fp-object>
      <fp-field :name="'commandOrNoop'">
        <command-or-noop :value="value.commandOrNoop">
        </command-or-noop>
      </fp-field>
      <fp-field :name="'dependency'">
        <frankenpaxos-seq :seq="value.dependency">
        </frankenpaxos-seq>
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

const phase2b_fast_component = {
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
      <fp-field :name="'voteValue'">
        <vote-value-proto :value="value.voteValue">
        </vote-value-proto>
      </fp-field>
    </fp-object>
  `,
};

const phase2b_classic_component = {
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
    'command-or-noop': command_or_noop_component,
    'vote-value': vote_value_component,
    'phase1b': phase1b_component,
    'phase2b-fast': phase2b_fast_component,
    'phase2b-classic': phase2b_classic_component,
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
    vertex_id_to_string: function(vertex_id) {
      return vertex_id.leaderIndex + "." + vertex_id.id;
    },

    nodes: function() {
      let ns = this.JsUtils.setToJs(this.node.actor.dependencyGraph.nodes);
      return ns.map(vertex_id => {
        return {
          id: this.vertex_id_to_string(vertex_id),
          label: this.vertex_id_to_string(vertex_id),
        };
      });
    },

    edges: function() {
      let es = this.JsUtils.setToJs(this.node.actor.dependencyGraph.edges);
      es = es.map(t => this.JsUtils.tupleToJs(t));
      es = es.map(t => {
        return {
          from: this.vertex_id_to_string(t[0]),
          to: this.vertex_id_to_string(t[1]),
        };
      });
      return es;
    },
  },

  template: `
    <div>
      <div>nextVertexId = {{node.actor.nextVertexId}}</div>

      <div>stateMachine = {{node.actor.stateMachine}}</div>

      <div>
        clientTable =
        <frankenpaxos-client-table :clientTable="node.actor.clientTable">
        </frankenpaxos-client-table>
      </div>

      <div>
        dependencyGraph =
        <frankenpaxos-graph
          style="height: 200px; border: 1pt solid black;"
          :nodes="nodes()"
          :edges="edges()"
          :options="options">
        </frankenpaxos-graph>
      </div>

      <div>
        states =
        <frankenpaxos-map :map="node.actor.states" v-slot="{value: state}">
          <div v-if="state.constructor.name.endsWith('Phase2Fast')">
            Phase2Fast
            <fp-object>
              <fp-field :name="'command'">
                <command :value="state.command"></command>
              </fp-field>
              <fp-field :name="'phase2bFasts'">
                <frankenpaxos-map
                  :map="state.phase2bFasts"
                  v-slot="{value: reply}">
                  <phase2b-fast :value="reply"></phase2b-fast>
                </frankenpaxos-map>
              </fp-field>
              <fp-field
                :name="'resendPhase1as'"
                :value="state.resendPhase1as">
              </fp-field>
            </fp-object>
          </div>

          <div v-if="state.constructor.name.endsWith('Phase1')">
            Phase1
            <fp-object>
              <fp-field :name="'round'" :value="state.round"></fp-field>
              <fp-field :name="'phase1bs'">
                <frankenpaxos-map
                  :map="state.phase1bs"
                  v-slot="{value: reply}">
                  <phase1b :value="reply"></phase1b>
                </frankenpaxos-map>
              </fp-field>
              <fp-field
                :name="'resendDependencyRequests'"
                :value="state.resendDependencyRequests">
              </fp-field>
            </fp-object>
          </div>

          <div v-if="state.constructor.name.endsWith('Phase2Classic')">
            Phase2Classic
            <fp-object>
              <fp-field :name="'round'" :value="state.round"></fp-field>
              <fp-field :name="'value'">
                <vote-value :value="state.value"></vote-value>
              </fp-field>
              <fp-field :name="'phase2bClassics'">
                <frankenpaxos-map
                  :map="state.phase2bClassics"
                  v-slot="{value: reply}">
                  <phase2b-classic :value="reply"></phase2b-classic>
                </frankenpaxos-map>
              </fp-field>
              <fp-field
                :name="'resendPhase2as'"
                :value="state.resendPhase2as">
              </fp-field>
            </fp-object>
          </div>

          <div v-if="state.constructor.name.endsWith('Committed')">
            Committed
            <fp-object>
              <fp-field :name="'commandOrNoop'">
                <command-or-noop :value="state.commandOrNoop"></command-or-noop>
              </fp-field>
              <fp-field :name="'dependencies'">
                <frankenpaxos-set :set="state.dependencies"></frankenpaxos-set>
              </fp-field>
            </fp-object>
          </div>
        </frankenpaxos-map>
      </div>
    </div>
  `,
};;

let dep_node_info = {
  props: {
    node: Object,
  },

  // TODO(mwhittaker): Improve display of conflictIndex.
  template: `
    <div>
      <div>conflictIndex = {{node.actor.conflictIndex}}</div>

      <div>
        dependenciesCache =
        <frankenpaxos-map
          :map="node.actor.dependenciesCache"
          v-slot="{value: deps}">
          <frankenpaxos-set :set="deps"></frankenpaxos-set>
        </frankenpaxos-map>
      </div>
    </div>
  `,
};

let acceptor_info = {
  props: {
    node: Object,
  },

  components: {
    'vote-value': vote_value_component,
  },

  template: `
    <div>
      states =
      <frankenpaxos-map :map="node.actor.states" v-slot="{value: state}">
        <fp-object>
          <fp-field :name="'round'" :value="state.round"></fp-field>
          <fp-field :name="'voteRound'" :value="state.voteRound"></fp-field>
          <fp-field :name="'voteValue'">
            <vote-value
              v-if="JsUtils.optionToJs(state.voteValue) !== undefined"
              :value="JsUtils.optionToJs(state.voteValue)">
            </vote-value>
            <div v-else>None</div>
          </fp-field>
        </fp-object>
      </frankenpaxos-map>
    </div>
  `,
};

// Main app ////////////////////////////////////////////////////////////////////
function make_nodes(UnanimousBPaxos, snap) {
  // https://flatuicolors.com/palette/defo
  let flat_red = '#e74c3c';
  let flat_blue = '#3498db';
  let flat_green = '#2ecc71';
  let flat_purple = '#9b59b6';
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

  let nodes = {};

  // Clients.
  const clients = [
    {actor: UnanimousBPaxos.client1, y: 100},
    {actor: UnanimousBPaxos.client2, y: 200},
    {actor: UnanimousBPaxos.client3, y: 300},
  ]
  for (const [index, client] of clients.entries()) {
    nodes[client.actor.address] = {
      actor: client.actor,
      color: flat_red,
      component: client_info,
      svgs: [
        snap.circle(50, client.y, 20).attr(colored(flat_red)),
        snap.text(50, client.y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Leaders.
  const leaders = [
    {actor: UnanimousBPaxos.leader1, y: 150},
    {actor: UnanimousBPaxos.leader2, y: 250},
  ]
  for (const [index, leader] of leaders.entries()) {
    nodes[leader.actor.address] = {
      actor: leader.actor,
      color: flat_blue,
      component: leader_info,
      svgs: [
        snap.circle(200, leader.y, 20).attr(colored(flat_blue)),
        snap.text(200, leader.y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Dependency service nodes.
  const dep_nodes = [
    {actor: UnanimousBPaxos.depServiceNode1, y: 75},
    {actor: UnanimousBPaxos.depServiceNode2, y: 175},
    {actor: UnanimousBPaxos.depServiceNode3, y: 275},
  ]
  for (const [index, dep_node] of dep_nodes.entries()) {
    nodes[dep_node.actor.address] = {
      actor: dep_node.actor,
      color: flat_purple,
      component: dep_node_info,
      svgs: [
        snap.circle(325, dep_node.y, 20).attr(colored(flat_purple)),
        snap.text(325, dep_node.y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Acceptors.
  const acceptors = [
    {actor: UnanimousBPaxos.acceptor1, y: 125},
    {actor: UnanimousBPaxos.acceptor2, y: 225},
    {actor: UnanimousBPaxos.acceptor3, y: 325},
  ]
  for (const [index, acceptor] of acceptors.entries()) {
    nodes[acceptor.actor.address] = {
      actor: acceptor.actor,
      color: flat_green,
      component: acceptor_info,
      svgs: [
        snap.circle(375, acceptor.y, 20).attr(colored(flat_green)),
        snap.text(375, acceptor.y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Node titles.
  snap.text(50, 25, 'Clients').attr({'text-anchor': 'middle'});
  snap.text(200, 25, 'Leaders').attr({'text-anchor': 'middle'});
  snap.text(325, 25, 'Dep Service/').attr({'text-anchor': 'middle'});
  snap.text(325, 45, 'Acceptors').attr({'text-anchor': 'middle'});

  return nodes;
}

function main() {
  const UnanimousBPaxos =
    frankenpaxos.unanimousbpaxos.UnanimousBPaxos.UnanimousBPaxos;
  const snap = Snap('#animation');
  const nodes = make_nodes(UnanimousBPaxos, snap);

  // Create the vue app.
  let vue_app = new Vue({
    el: '#app',

    data: {
      nodes: nodes,
      node: nodes[UnanimousBPaxos.client1.address],
      transport: UnanimousBPaxos.transport,
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
        let speed = 200 + (Math.random() * 50); // px per second.

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
