
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
      <div><strong>pendingCommand</strong>: {{node.actor.pendingCommand}}</div>
      <button v-on:click="propose">Propose</button>
      <input v-model="proposal" v-on:keyup.enter="propose"></input>
    </div>
  `,
};

let proposer_info = {
  props: ['node'],

  template: `
    <div>
      <div><strong>id</strong>: {{node.actor.id}}</div>
    </div>
  `,
};

let executor_info = {
  props: ['node'],

  template: `
    <div>
      <div><strong>id</strong>: {{node.actor.id}}</div>
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

function make_nodes(SPaxosDecouple, snap) {
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
  nodes[SPaxosDecouple.client1.address] = {
    actor: SPaxosDecouple.client1,
    svgs: [
      snap.circle(clients_x, 100, 20).attr(colored(flat_red)),
      snap.text(clients_x, 102, '1').attr(number_style),
    ],
    color: flat_red,
    component: client_info,
  }
  nodes[SPaxosDecouple.client2.address] = {
    actor: SPaxosDecouple.client2,
    svgs: [
      snap.circle(clients_x, 200, 20).attr(colored(flat_red)),
      snap.text(clients_x, 202, '2').attr(number_style),
    ],
    color: flat_red,
    component: client_info,
  }
  nodes[SPaxosDecouple.client3.address] = {
    actor: SPaxosDecouple.client3,
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
  nodes[SPaxosDecouple.leader1.address] = {
    actor: SPaxosDecouple.leader1,
    svgs: [
      snap.circle(leaders_x, leader1_y, 20).attr(colored(flat_blue)),
      snap.text(leaders_x, leader1_y, '1').attr(number_style),
    ],
    color: flat_blue,
    component: leader_info,
  }

  // Proposers
  let proposers_x = 350;

  nodes[SPaxosDecouple.proposer1.address] = {
    actor: SPaxosDecouple.proposer1,
    svgs: [
      snap.circle(proposers_x, 100, 20).attr(colored(flat_blue)),
      snap.text(proposers_x, 102, '2').attr(number_style),
    ],
    color: flat_blue,
    component: proposer_info,
  }

  nodes[SPaxosDecouple.proposer2.address] = {
    actor: SPaxosDecouple.proposer2,
    svgs: [
      snap.circle(proposers_x, 200, 20).attr(colored(flat_blue)),
      snap.text(proposers_x, 202, '2').attr(number_style),
    ],
    color: flat_blue,
    component: proposer_info,
  }

  nodes[SPaxosDecouple.proposer3.address] = {
    actor: SPaxosDecouple.proposer3,
    svgs: [
      snap.circle(proposers_x, 300, 20).attr(colored(flat_blue)),
      snap.text(proposers_x, 302, '2').attr(number_style),
    ],
    color: flat_blue,
    component: proposer_info,
  }

  // Acceptors.
  let acceptors_x = 350;
  nodes[SPaxosDecouple.acceptor1.address] = {
    actor: SPaxosDecouple.acceptor1,
    svgs: [
      snap.circle(acceptors_x, 100, 20).attr(colored(flat_green)),
      snap.text(acceptors_x, 102, '1').attr(number_style),
    ],
    color: flat_green,
    component: acceptor_info,
  }
  nodes[SPaxosDecouple.acceptor2.address] = {
    actor: SPaxosDecouple.acceptor2,
    svgs: [
      snap.circle(acceptors_x, 200, 20).attr(colored(flat_green)),
      snap.text(acceptors_x, 202, '2').attr(number_style),
    ],
    color: flat_green,
    component: acceptor_info,
  }
  nodes[SPaxosDecouple.acceptor3.address] = {
    actor: SPaxosDecouple.acceptor3,
    svgs: [
      snap.circle(acceptors_x, 300, 20).attr(colored(flat_green)),
      snap.text(acceptors_x, 302, '3').attr(number_style),
    ],
    color: flat_green,
    component: acceptor_info,
  }

  // Executors.
  let executors_x = 450;
  nodes[SPaxosDecouple.executor1.address] = {
    actor: SPaxosDecouple.executor1,
    svgs: [
      snap.circle(acceptors_x, 100, 20).attr(colored(flat_green)),
      snap.text(acceptors_x, 102, '1').attr(number_style),
    ],
    color: flat_green,
    component: executor_info,
  }
  nodes[SPaxosDecouple.executor2.address] = {
    actor: SPaxosDecouple.executor2,
    svgs: [
      snap.circle(acceptors_x, 200, 20).attr(colored(flat_green)),
      snap.text(acceptors_x, 202, '2').attr(number_style),
    ],
    color: flat_green,
    component: executor_info,
  }
  nodes[SPaxosDecouple.executor3.address] = {
    actor: SPaxosDecouple.executor3,
    svgs: [
      snap.circle(acceptors_x, 300, 20).attr(colored(flat_green)),
      snap.text(acceptors_x, 302, '3').attr(number_style),
    ],
    color: flat_green,
    component: executor_info,
  }

  // Node titles.
  snap.text(50, 15, 'Clients').attr({'text-anchor': 'middle'});
  snap.text(200, 15, 'Proposers').attr({'text-anchor': 'middle'});
  snap.text(350, 15, 'Acceptors').attr({'text-anchor': 'middle'});

  return nodes
}

function main() {
  let SPaxosDecouple =
      frankenpaxos.spaxosdecouple.TweenedSPaxosDecouple.SPaxosDecouple;
  let snap = Snap('#animation');
  let nodes = make_nodes(SPaxosDecouple, snap);

  // Create the vue app.
  let vue_app = new Vue({
    el: '#app',

    data: {
      nodes: nodes,
      node: nodes[SPaxosDecouple.client1.address],
      transport: SPaxosDecouple.transport,
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
