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
      this.node.actor.propose(this.proposal);
      this.proposal = "";
    }
  },

  template: `
    <div>
      <div>pendingCommand = {{node.actor.pendingCommand}}</div>
      <button v-on:click="propose">Propose</button>
      <input v-model="proposal" v-on:keyup.enter="propose"></input>
    </div>
  `,
};

let replica_info = {
  props: ['node'],

  template: `
    <div>
      <div>nextAvailableInstance = {{node.actor.nextAvailableInstance}}</div>
      <div>largestBallot = {{node.actor.largestBallot}}</div>
      <!-- <div>stateMachine = {{node.actor.stateMachine}}</div> -->
      <!-- <div>dependencyGraph = {{node.actor.dependencyGraph}}</div> -->

      <div>
        cmdLog =
        <frankenpaxos-map :map=node.actor.cmdLog v-slot="{value: entry}">
          <div v-if="entry.constructor.name.endsWith('$NoCommandEntry')">
            <strong>NoCommandEntry</strong>
            <fp-object v-slot="{let: noCommand}" :value="entry">
              <fp-field :name="'ballot'" :value="noCommand.ballot"></fp-field>
            </fp-object>
          </div>

          <div v-if="entry.constructor.name.endsWith('$PreAcceptedEntry')">
            <strong>PreAcceptedEntry</strong>
            <fp-object v-slot="{let: preAccepted}" :value="entry">
              <fp-field :name="'ballot'" :value="preAccepted.ballot"></fp-field>
              <fp-field :name="'voteBallot'" :value="preAccepted.voteBallot"></fp-field>
              <fp-field v-slot="{let: triple}" :name="'triple'" :value="preAccepted.triple">
                <fp-object :value="triple">
                  <fp-field :name="'commandOrNoop'" :value="triple.commandOrNoop" :show_at_start="false"></fp-field>
                  <fp-field :name="'sequenceNumber'" :value="triple.sequenceNumber"></fp-field>
                  <fp-field :name="'dependencies'" :value="triple.dependencies"></fp-field>
                </fp-object>
              </fp-field>
            </fp-object>
          </div>

          <div v-if="entry.constructor.name.endsWith('$AcceptedEntry')">
            <strong>AcceptedEntry</strong>
            <fp-object v-slot="{let: accepted}" :value="entry">
              <fp-field :name="'ballot'" :value="accepted.ballot"></fp-field>
              <fp-field :name="'voteBallot'" :value="accepted.voteBallot"></fp-field>
              <fp-field v-slot="{let: triple}" :name="'triple'" :value="accepted.triple">
                <fp-object :value="triple">
                  <fp-field :name="'commandOrNoop'" :value="triple.commandOrNoop" :show_at_start="false"></fp-field>
                  <fp-field :name="'sequenceNumber'" :value="triple.sequenceNumber"></fp-field>
                  <fp-field :name="'dependencies'" :value="triple.dependencies"></fp-field>
                </fp-object>
              </fp-field>
            </fp-object>
          </div>

          <div v-if="entry.constructor.name.endsWith('$CommittedEntry')">
            <strong>CommittedEntry</strong>
            <fp-object v-slot="{let: committed}" :value="entry">
              <fp-field v-slot="{let: triple}" :name="'triple'" :value="committed.triple">
                <fp-object :value="triple">
                  <fp-field :name="'commandOrNoop'" :value="triple.commandOrNoop" :show_at_start="false"></fp-field>
                  <fp-field :name="'sequenceNumber'" :value="triple.sequenceNumber"></fp-field>
                  <fp-field :name="'dependencies'" :value="triple.dependencies"></fp-field>
                </fp-object>
              </fp-field>
            </fp-object>
          </div>
        </frankenpaxos-map>
      </div>

      <div>
        leaderStates =
        <frankenpaxos-map :map=node.actor.leaderStates>
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
      snap.circle(replica_x + 100, 300, 20).attr(colored(flat_blue)),
      snap.text(replica_x + 100, 300, '3').attr(number_style),
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
  let snap = Snap('#tweened_animation');
  let nodes = make_nodes(EPaxos, snap);

  // Create the vue app.
  let vue_app = new Vue({
    el: '#tweened_app',

    data: {
      nodes: nodes,
      node: nodes[EPaxos.client1.address],
      transport: EPaxos.transport,
      time_scale: 1,
      auto_deliver_messages: true,
      auto_start_timers: true,
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
