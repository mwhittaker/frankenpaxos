let election_info = {
  props: ['node'],

  template: `
    <div>
      <div>round = {{node.actor.round}}</div>
      <div>state = {{node.actor.state}}</div>
    </div>
  `,
};

let heartbeat_info = {
  props: ['node'],

  template: `
    <div>
      <div>numRetries = {{node.actor.numRetries}}</div>
      <div>alive = {{node.actor.alive}}</div>
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
      this.node.actor.propose(this.proposal);
      this.proposal = "";
    }
  },

  template: `
    <div>
      <div>id = {{node.actor.id}}</div>
      <div>round = {{node.actor.round}}</div>
      <div>pendingCommand = {{node.actor.pendingCommand}}</div>
      <button v-on:click="propose">Propose</button>
      <input v-model="proposal" v-on:keyup.enter="propose"></input>
    </div>
  `,
};

let leader_info = {
  props: ['node'],

  template: `
    <div>
      <div>round = {{node.actor.round}}</div>
      <div>log = {{node.actor.log}}</div>
      <div>clientTable = {{node.actor.clientTable}}</div>
      <div>chosenWatermark = {{node.actor.chosenWatermark}}</div>
      <div>nextSlot = {{node.actor.nextSlot}}</div>
      <div>state = {{node.actor.state}}</div>
    </div>
  `,
};

let acceptor_info = {
  props: ['node'],

  template: `
    <div>
      <div>round = {{node.actor.round}}</div>
      <div>log = {{node.actor.log}}</div>
      <div>nextSlot = {{node.actor.nextSlot}}</div>
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
  }
  nodes[FastMultiPaxos.client2.address] = {
    actor: FastMultiPaxos.client2,
    svgs: [
      snap.circle(clients_x, 200, 20).attr(colored(flat_red)),
      snap.text(clients_x, 202, '2').attr(number_style),
    ],
  }
  nodes[FastMultiPaxos.client3.address] = {
    actor: FastMultiPaxos.client3,
    svgs: [
      snap.circle(clients_x, 300, 20).attr(colored(flat_red)),
      snap.text(clients_x, 302, '3').attr(number_style),
    ],
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
  }
  nodes[FastMultiPaxos.leader1.electionAddress] = {
    actor: FastMultiPaxos.leader1.election,
    svgs: [
      snap.circle(leaders_x + 50, leader1_y, 15).attr(colored(flat_blue)),
      snap.text(leaders_x + 50, leader1_y, 'e').attr(small_number_style),
    ],
  }
  nodes[FastMultiPaxos.leader1.heartbeatAddress] = {
    actor: FastMultiPaxos.leader1.heartbeat,
    svgs: [
      snap.circle(leaders_x + 100, leader1_y, 15).attr(colored(flat_blue)),
      snap.text(leaders_x + 100, leader1_y, 'h').attr(small_number_style),
    ],
  }

  let leader2_y = 350;
  nodes[FastMultiPaxos.leader2.address] = {
    actor: FastMultiPaxos.leader2,
    svgs: [
      snap.circle(leaders_x, leader2_y, 20).attr(colored(flat_blue)),
      snap.text(leaders_x, leader2_y, '2').attr(number_style),
    ],
  }
  nodes[FastMultiPaxos.leader2.electionAddress] = {
    actor: FastMultiPaxos.leader2.election,
    svgs: [
      snap.circle(leaders_x + 50, leader2_y, 15).attr(colored(flat_blue)),
      snap.text(leaders_x + 50, leader2_y, 'e').attr(small_number_style),
    ],
  }
  nodes[FastMultiPaxos.leader2.heartbeatAddress] = {
    actor: FastMultiPaxos.leader2.heartbeat,
    svgs: [
      snap.circle(leaders_x + 100, leader2_y, 15).attr(colored(flat_blue)),
      snap.text(leaders_x + 100, leader2_y, 'h').attr(small_number_style),
    ],
  }

  // Acceptors.
  let acceptors_x = 350;
  nodes[FastMultiPaxos.acceptor1.address] = {
    actor: FastMultiPaxos.acceptor1,
    svgs: [
      snap.circle(acceptors_x, 100, 20).attr(colored(flat_green)),
      snap.text(acceptors_x, 102, '1').attr(number_style),
    ],
  }
  nodes[FastMultiPaxos.acceptor2.address] = {
    actor: FastMultiPaxos.acceptor2,
    svgs: [
      snap.circle(acceptors_x, 200, 20).attr(colored(flat_green)),
      snap.text(acceptors_x, 202, '2').attr(number_style),
    ],
  }
  nodes[FastMultiPaxos.acceptor3.address] = {
    actor: FastMultiPaxos.acceptor3,
    svgs: [
      snap.circle(acceptors_x, 300, 20).attr(colored(flat_green)),
      snap.text(acceptors_x, 302, '3').attr(number_style),
    ],
  }

  // Acceptor heartbeats.
  nodes[FastMultiPaxos.acceptor1.heartbeatAddress] = {
    actor: FastMultiPaxos.acceptor1.heartbeat,
    svgs: [
      snap.circle(acceptors_x - 40, 100 - 40, 15).attr(colored(flat_green)),
      snap.text(acceptors_x - 40, 100 - 40, 'h').attr(small_number_style),
    ],
  }
  nodes[FastMultiPaxos.acceptor2.heartbeatAddress] = {
    actor: FastMultiPaxos.acceptor2.heartbeat,
    svgs: [
      snap.circle(acceptors_x - 40, 200 - 40, 15).attr(colored(flat_green)),
      snap.text(acceptors_x - 40, 200 - 40, 'h').attr(small_number_style),
    ],
  }
  nodes[FastMultiPaxos.acceptor3.heartbeatAddress] = {
    actor: FastMultiPaxos.acceptor3.heartbeat,
    svgs: [
      snap.circle(acceptors_x - 40, 300 - 40, 15).attr(colored(flat_green)),
      snap.text(acceptors_x - 40, 300 - 40, 'h').attr(small_number_style),
    ],
  }

  // Node titles.
  snap.text(50, 15, 'Clients').attr({'text-anchor': 'middle'});
  snap.text(200, 15, 'Leaders').attr({'text-anchor': 'middle'});
  snap.text(350, 15, 'Acceptors').attr({'text-anchor': 'middle'});

  return nodes
}

function make_app(FastMultiPaxos, snap, app_id) {
  let nodes = make_nodes(FastMultiPaxos, snap);

  // Create the vue app.
  let vue_app = new Vue({
    el: app_id,

    data: {
      node: nodes[FastMultiPaxos.client1.address],
      transport: FastMultiPaxos.transport,
      send_message: (message, callback) => {
        // console.log(message.src.address);
        // console.log(message.dst.address);
        // console.log("");
        let src = nodes[message.src];
        let dst = nodes[message.dst];
        let svg_message =
          snap.circle(src.svgs[0].attr("cx"), src.svgs[0].attr("cy"), 9)
              .attr({fill: '#2c3e50'});
        snap.prepend(svg_message);
        svg_message.animate(
          {cx: dst.svgs[0].attr("cx"), cy: dst.svgs[0].attr("cy")},
          1000 + Math.random() * 200,
          callback);
      }
    },

    computed: {
      current_component: function() {
        if (this.node.actor.address.address.includes('Election')) {
          return election_info;
        } else if (this.node.actor.address.address.includes('Heartbeat')) {
          return heartbeat_info;
        } else if (this.node.actor.address.address.includes('Client')) {
          return client_info;
        } else if (this.node.actor.address.address.includes('Leader')) {
          return leader_info;
        } else if (this.node.actor.address.address.includes('Acceptor')) {
          return acceptor_info;
        } else {
          // Impossible!
          console.assert(false);
        }
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

function main() {
  // make_app(frankenpaxos.fastpaxos.SimulatedFastMultiPaxos.FastMultiPaxos,
  //          Snap('#simulated_animation'),
  //          '#simulated_app');

  make_app(frankenpaxos.fastpaxos.ClickthroughFastMultiPaxos.FastMultiPaxos,
           Snap('#clickthrough_animation'),
           '#clickthrough_app');
}

window.onload = main
