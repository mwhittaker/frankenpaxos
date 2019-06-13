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
      <div><strong>proposed value</strong>: {{node.actor.proposedValue}}</div>
      <div><strong>state</strong>: {{node.actor.state}}</div>
      <button v-on:click="propose">Propose</button>
      <input v-model="proposal" v-on:keyup.enter="propose"></input>
    </div>
  `,
};

let replica_info = {
  props: ['node'],

  template: `
    <div>
      <div><strong>state</strong>: {{node.actor.state}}</div>
      <div><strong>slotIn</strong>: {{node.actor.slotIn}}</div>
      <div><strong>slotOut</strong>: {{node.actor.slotOut}}</div>
      <div><strong>requests</strong>: {{node.actor.requests}}</div>
      <div><strong>proposals</strong>: {{node.actor.proposals}}</div>
      <div><strong>decisions</strong>: {{node.actor.decisions}}</div>
    </div>
  `,
};

let leader_info = {
  props: ['node'],

  template: `
    <div>
      <div><strong>ballotNumber</strong>: {{node.actor.ballotNumber}}</div>
      <div><strong>active</strong>: {{node.actor.active}}</div>
      <div><strong>proposals</strong>: {{node.actor.proposals}}</div>
      <div><strong>waitForCommander</strong>: {{node.actor.waitForCommander}}</div>
      <div><strong>waitForScout</strong>: {{node.actor.waitForScout}}</div>
      <div><strong>scoutProposalValues</strong>: {{node.actor.scoutProposalValues}}</div>
      <div><strong>activateScout</strong>: {{node.actor.activateScout}}</div>
    </div>
  `,
};


let acceptor_info = {
  props: ['node'],

  template: `
    <div>
      <div><strong>ballotNumber</strong>: {{node.actor.ballotNumber}}</div>
      <div><strong>accepted</strong>: {{node.actor.accepted}}</div>
    </div>
  `,
};

function make_nodes(MultiPaxos, snap) {
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

  let client_x = 50;
  let replica_x = 150;
  let leader_x = 250;
  let acceptor_x = 350;

  let nodes = {};

  // Clients.
  nodes[MultiPaxos.client1.address] = {
    actor: MultiPaxos.client1,
    svgs: [
      snap.circle(client_x, 50, 20).attr(colored(flat_red)),
      snap.text(client_x, 52, '1').attr(number_style),
    ],
    color: flat_red,
    component: client_info,
  };
  nodes[MultiPaxos.client2.address] = {
    actor: MultiPaxos.client2,
    svgs: [
      snap.circle(client_x, 150, 20).attr(colored(flat_red)),
      snap.text(client_x, 152, '2').attr(number_style),
    ],
    color: flat_red,
    component: client_info,
  };
  nodes[MultiPaxos.client3.address] = {
    actor: MultiPaxos.client3,
    svgs: [
      snap.circle(client_x, 250, 20).attr(colored(flat_red)),
      snap.text(client_x, 252, '3').attr(number_style),
    ],
    color: flat_red,
    component: client_info,
  };

  // Replicas.
  nodes[MultiPaxos.replica1.address] = {
    actor: MultiPaxos.replica1,
    svgs: [
      snap.circle(replica_x, 100, 20).attr(colored(flat_blue)),
      snap.text(replica_x, 102, '1').attr(number_style),
    ],
    color: flat_blue,
    component: replica_info,
  };
  nodes[MultiPaxos.replica2.address] = {
    actor: MultiPaxos.replica2,
    svgs: [
      snap.circle(replica_x, 200, 20).attr(colored(flat_blue)),
      snap.text(replica_x, 202, '2').attr(number_style),
    ],
    color: flat_blue,
    component: replica_info,
  };

  // Leaders.
  nodes[MultiPaxos.leader1.address] = {
    actor: MultiPaxos.leader1,
    svgs: [
      snap.circle(leader_x, 100, 20).attr(colored(flat_orange)),
      snap.text(leader_x, 102, '1').attr(number_style),
    ],
    color: flat_orange,
    component: leader_info,
  };
  nodes[MultiPaxos.leader2.address] = {
    actor: MultiPaxos.leader2,
    svgs: [
      snap.circle(leader_x, 200, 20).attr(colored(flat_orange)),
      snap.text(leader_x, 202, '2').attr(number_style),
    ],
    color: flat_orange,
    component: leader_info,
  };

  // Acceptors.
  nodes[MultiPaxos.acceptor1.address] = {
    actor: MultiPaxos.acceptor1,
    svgs: [
      snap.circle(acceptor_x, 50, 20).attr(colored(flat_green)),
      snap.text(acceptor_x, 52, '1').attr(number_style),
    ],
    color: flat_green,
    component: acceptor_info,
  };
  nodes[MultiPaxos.acceptor2.address] = {
    actor: MultiPaxos.acceptor2,
    svgs: [
      snap.circle(acceptor_x, 150, 20).attr(colored(flat_green)),
      snap.text(acceptor_x, 152, '2').attr(number_style),
    ],
    color: flat_green,
    component: acceptor_info,
  };
  nodes[MultiPaxos.acceptor3.address] = {
    actor: MultiPaxos.acceptor3,
    svgs: [
      snap.circle(acceptor_x, 250, 20).attr(colored(flat_green)),
      snap.text(acceptor_x, 252, '3').attr(number_style),
    ],
    color: flat_green,
    component: acceptor_info,
  };

  // Node titles.
  snap.text(client_x, 15, 'Clients').attr({'text-anchor': 'middle'});
  snap.text(replica_x, 15, 'Replicas').attr({'text-anchor': 'middle'});
  snap.text(leader_x, 15, 'Leaders').attr({'text-anchor': 'middle'});
  snap.text(acceptor_x, 15, 'Acceptors').attr({'text-anchor': 'middle'});

  return nodes;
}

function main() {
  let MultiPaxos = frankenpaxos.multipaxos.TweenedMultiPaxos.MultiPaxos;
  let snap = Snap('#animation');
  let nodes = make_nodes(MultiPaxos, snap);

  // Create the vue app.
  let vue_app = new Vue({
    el: '#app',

    data: {
      nodes: nodes,
      node: nodes[MultiPaxos.client1.address],
      transport: MultiPaxos.transport,
      settings: {
        time_scale: 1,
        auto_deliver_messages: true,
        auto_start_timers: true,
      },
    },

    methods: {
      send_message: (message, callback) => {
        let src = nodes[message.src];
        let dst = nodes[message.dst];
        let src_x = src.svgs[0].attr("cx");
        let src_y = src.svgs[0].attr("cy");
        let dst_x = dst.svgs[0].attr("cx");
        let dst_y = dst.svgs[0].attr("cy");

        let svg_message = snap.circle(src_x, src_y, 9).attr({fill: '#2c3e50'});
        snap.prepend(svg_message);
        let duration = (250 + Math.random() * 200) / 1000;
        return TweenMax.to(svg_message.node, duration, {
          attr: { cx: dst_x, cy: dst_y },
          ease: Linear.easeNone,
          onComplete: () => { svg_message.remove(); },
        });
      },

      partition: function(address) {
        this.nodes[address].svgs[0].attr({fill: "#7f8c8d"});
      },

      unpartition: function(address) {
        this.nodes[address].svgs[0].attr({fill: this.nodes[address].color});
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
