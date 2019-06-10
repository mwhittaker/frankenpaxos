let election_info = {
  props: ['node'],

  template: `
    <div>
      <div><strong>round</strong>: {{node.actor.round}}</div>
      <div><strong>state</strong>: {{node.actor.state}}</div>
    </div>
  `,
};

let heartbeat_info = {
  props: ['node'],

  template: `
    <div>
      <div>
        <strong>numRetries</strong>:
        <frankenpaxos-map :map="node.actor.numRetries"></frankenpaxos-map>
      </div>

      <div>
        <strong>networkDelayNanos</strong>:
        <frankenpaxos-map :map="node.actor.networkDelayNanos">
        </frankenpaxos-map>
      </div>

      <div>
        <strong>alive</strong>:
        <frankenpaxos-set :set="node.actor.alive"></frankenpaxos-set>
      </div>
    </div>
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
    }
  },

  template: `
    <div>
      <div><strong>id</strong>: {{node.actor.id}}</div>
      <div><strong>round</strong>: {{node.actor.round}}</div>
      <div><strong>pendingCommand</strong>: {{node.actor.pendingCommand}}</div>
      <button v-on:click="propose">Propose</button>
      <input v-model="proposal" v-on:keyup.enter="propose"></input>
    </div>
  `,
};

let leader_info = {
  props: ['node'],

  template: `
    <div>
      <div><strong>round</strong>: {{node.actor.round}}</div>
      <div><strong>chosenWatermark</strong>: {{node.actor.chosenWatermark}}</div>
      <div><strong>nextSlot</strong>: {{node.actor.nextSlot}}</div>
      <div><strong>stateMachine</strong>: {{node.actor.stateMachine}}</div>
      <div><strong>clientTable</strong>:
           <frankenpaxos-map :map=node.actor.clientTable></frankenpaxos-map>
      </div>
      <div><strong>state</strong>: {{node.actor.state}}</div>
      <div>
        <strong>phase1bs</strong>:
        <frankenpaxos-map :map=node.actor.state.phase1bs></frankenpaxos-map>
      </div>
      <div>
        <strong>pendingProposals</strong>:
        <frankenpaxos-seq :seq=node.actor.state.pendingProposals>
        </frankenpaxos-seq>
      </div>
      <div>
        <strong>pendingEntries</strong>:
        <frankenpaxos-map :map=node.actor.state.pendingEntries>
        </frankenpaxos-map>
      </div>
      <div>
        <strong>phase2bs</strong>:
        <frankenpaxos-map :map=node.actor.state.phase2bs v-slot="slotProps">
          <frankenpaxos-map :map="slotProps.value">
          </frankenpaxos-map>
        </frankenpaxos-map>
      </div>
      <div>
        <strong>phase2aBuffer</strong>:
        <frankenpaxos-seq :seq=node.actor.state.phase2aBuffer>
        </frankenpaxos-seq>
      </div>
      <div>
        <strong>valueChosenBuffer</strong>:
        <frankenpaxos-seq :seq=node.actor.state.valueChosenBuffer>
        </frankenpaxos-seq>
      </div>
      <div>
        <strong>log</strong>:
        <frankenpaxos-map :map=node.actor.log></frankenpaxos-map>
      </div>
    </div>
  `,
};

let acceptor_info = {
  props: ['node'],

  template: `
    <div>
      <div><strong>round</strong>: {{node.actor.round}}</div>
      <div><strong>nextSlot</strong>: {{node.actor.nextSlot}}</div>
      <div>
        <strong>bufferedProposeRequests</strong>:
        <frankenpaxos-seq :seq="node.actor.bufferedProposeRequests"
                          v-slot="slotProps">
          <frankenpaxos-tuple :tuple="slotProps.value">
          </frankenpaxos-tuple>
        </frankenpaxos-seq>
      </div>
      <div>
        <strong>log prefix</strong>:
        <frankenpaxos-map :map="node.actor.log.prefix()"></frankenpaxos-map>
      </div>
      <div><strong>log tail</strong>: {{node.actor.log.tail}}</div>
    </div>
  `,
};

function make_nodes(FastMultiPaxos, snap) {
  // https://flatuicolors.com/palette/defo
  let flat_red = '#e74c3c';
  let flat_blue = '#3498db';
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

  let small_number_style = {
    'text-anchor': 'middle',
    'alignment-baseline': 'middle',
    'font-size': '15pt',
    'fill': 'black',
  }

  let nodes = {};

  // Clients.
  let clients_x = 50;
  nodes[FastMultiPaxos.client1.address] = {
    actor: FastMultiPaxos.client1,
    svgs: [
      snap.circle(clients_x, 100, 20).attr(colored(flat_red)),
      snap.text(clients_x, 102, '1').attr(number_style),
    ],
    color: flat_red,
    component: client_info,
  }
  nodes[FastMultiPaxos.client2.address] = {
    actor: FastMultiPaxos.client2,
    svgs: [
      snap.circle(clients_x, 200, 20).attr(colored(flat_red)),
      snap.text(clients_x, 202, '2').attr(number_style),
    ],
    color: flat_red,
    component: client_info,
  }
  nodes[FastMultiPaxos.client3.address] = {
    actor: FastMultiPaxos.client3,
    svgs: [
      snap.circle(clients_x, 300, 20).attr(colored(flat_red)),
      snap.text(clients_x, 302, '3').attr(number_style),
    ],
    color: flat_red,
    component: client_info,
  }

  // Leaders.
  let leaders_x = 150;
  let leader1_y = 50;
  nodes[FastMultiPaxos.leader1.address] = {
    actor: FastMultiPaxos.leader1,
    svgs: [
      snap.circle(leaders_x, leader1_y, 20).attr(colored(flat_blue)),
      snap.text(leaders_x, leader1_y, '1').attr(number_style),
    ],
    color: flat_blue,
    component: leader_info,
  }
  nodes[FastMultiPaxos.leader1.electionAddress] = {
    actor: FastMultiPaxos.leader1.election,
    svgs: [
      snap.circle(leaders_x + 50, leader1_y, 15).attr(colored(flat_blue)),
      snap.text(leaders_x + 50, leader1_y, 'e').attr(small_number_style),
    ],
    color: flat_blue,
    component: election_info,
  }
  nodes[FastMultiPaxos.leader1.heartbeatAddress] = {
    actor: FastMultiPaxos.leader1.heartbeat,
    svgs: [
      snap.circle(leaders_x + 100, leader1_y, 15).attr(colored(flat_blue)),
      snap.text(leaders_x + 100, leader1_y, 'h').attr(small_number_style),
    ],
    color: flat_blue,
    component: heartbeat_info,
  }

  let leader2_y = 350;
  nodes[FastMultiPaxos.leader2.address] = {
    actor: FastMultiPaxos.leader2,
    svgs: [
      snap.circle(leaders_x, leader2_y, 20).attr(colored(flat_blue)),
      snap.text(leaders_x, leader2_y, '2').attr(number_style),
    ],
    color: flat_blue,
    component: leader_info,
  }
  nodes[FastMultiPaxos.leader2.electionAddress] = {
    actor: FastMultiPaxos.leader2.election,
    svgs: [
      snap.circle(leaders_x + 50, leader2_y, 15).attr(colored(flat_blue)),
      snap.text(leaders_x + 50, leader2_y, 'e').attr(small_number_style),
    ],
    color: flat_blue,
    component: election_info,
  }
  nodes[FastMultiPaxos.leader2.heartbeatAddress] = {
    actor: FastMultiPaxos.leader2.heartbeat,
    svgs: [
      snap.circle(leaders_x + 100, leader2_y, 15).attr(colored(flat_blue)),
      snap.text(leaders_x + 100, leader2_y, 'h').attr(small_number_style),
    ],
    color: flat_blue,
    component: heartbeat_info,
  }

  // Acceptors.
  let acceptors_x = 350;
  nodes[FastMultiPaxos.acceptor1.address] = {
    actor: FastMultiPaxos.acceptor1,
    svgs: [
      snap.circle(acceptors_x, 100, 20).attr(colored(flat_green)),
      snap.text(acceptors_x, 102, '1').attr(number_style),
    ],
    color: flat_green,
    component: acceptor_info,
  }
  nodes[FastMultiPaxos.acceptor2.address] = {
    actor: FastMultiPaxos.acceptor2,
    svgs: [
      snap.circle(acceptors_x, 200, 20).attr(colored(flat_green)),
      snap.text(acceptors_x, 202, '2').attr(number_style),
    ],
    color: flat_green,
    component: acceptor_info,
  }
  nodes[FastMultiPaxos.acceptor3.address] = {
    actor: FastMultiPaxos.acceptor3,
    svgs: [
      snap.circle(acceptors_x, 300, 20).attr(colored(flat_green)),
      snap.text(acceptors_x, 302, '3').attr(number_style),
    ],
    color: flat_green,
    component: acceptor_info,
  }

  // Acceptor heartbeats.
  nodes[FastMultiPaxos.acceptor1.heartbeatAddress] = {
    actor: FastMultiPaxos.acceptor1.heartbeat,
    svgs: [
      snap.circle(acceptors_x - 40, 100 - 40, 15).attr(colored(flat_green)),
      snap.text(acceptors_x - 40, 100 - 40, 'h').attr(small_number_style),
    ],
    color: flat_green,
    component: heartbeat_info,
  }
  nodes[FastMultiPaxos.acceptor2.heartbeatAddress] = {
    actor: FastMultiPaxos.acceptor2.heartbeat,
    svgs: [
      snap.circle(acceptors_x - 40, 200 - 40, 15).attr(colored(flat_green)),
      snap.text(acceptors_x - 40, 200 - 40, 'h').attr(small_number_style),
    ],
    color: flat_green,
    component: heartbeat_info,
  }
  nodes[FastMultiPaxos.acceptor3.heartbeatAddress] = {
    actor: FastMultiPaxos.acceptor3.heartbeat,
    svgs: [
      snap.circle(acceptors_x - 40, 300 - 40, 15).attr(colored(flat_green)),
      snap.text(acceptors_x - 40, 300 - 40, 'h').attr(small_number_style),
    ],
    color: flat_green,
    component: heartbeat_info,
  }

  // Node titles.
  snap.text(50, 15, 'Clients').attr({'text-anchor': 'middle'});
  snap.text(200, 15, 'Leaders').attr({'text-anchor': 'middle'});
  snap.text(350, 15, 'Acceptors').attr({'text-anchor': 'middle'});

  return nodes
}

function main() {
  let FastMultiPaxos =
    frankenpaxos.fastmultipaxos.TweenedFastMultiPaxos.FastMultiPaxos;
  let snap = Snap('#tweened_animation');
  let nodes = make_nodes(FastMultiPaxos, snap);

  // Create the vue app.
  let vue_app = new Vue({
    el: '#tweened_app',

    data: {
      nodes: nodes,
      node: nodes[FastMultiPaxos.client1.address],
      transport: FastMultiPaxos.transport,
      time_scale: 1,
      auto_deliver_messages: true,
      auto_start_timers: true,
      record_history_for_unit_tests: false,
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

    watch: {
      record_history_for_unit_tests: function(b) {
        this.transport.recordHistory = b;
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
