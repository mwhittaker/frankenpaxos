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
      <div>proposed value: {{node.actor.proposedValue}}</div>
      <div>chosen value: {{node.actor.chosenValue}}</div>
      <button v-on:click="propose">Propose</button>
      <input v-model="proposal" v-on:keyup.enter="propose"></input>
    </div>
  `,
};

let acceptor_info = {
  props: ['node'],

  template: `
    <div>
      <div>round = {{node.actor.round}}</div>
      <div>voteRound = {{node.actor.voteRound}}</div>
      <div>voteValue = {{node.actor.voteValue}}</div>
    </div>
  `,
};

let abbreviated_acceptor_info = {
  props: ['node'],

  template: `
    <div class="column">
      <div>{{node.actor.address.address}}</div>
      <div>{{node.actor.round}}</div>
      <div>{{node.actor.voteRound}}</div>
      <div>{{node.actor.voteValue}}</div>
    </div>
  `,
};


function make_nodes(Paxos, snap) {
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

  let nodes = {};

  // Clients.
  nodes[Paxos.client1.address] = {
    actor: Paxos.client1,
    svgs: [
      snap.circle(50, 50, 20).attr(colored(flat_red)),
      snap.text(50, 52, '1').attr(number_style),
    ],
  }
  nodes[Paxos.client2.address] = {
    actor: Paxos.client2,
    svgs: [
      snap.circle(50, 150, 20).attr(colored(flat_red)),
      snap.text(50, 152, '2').attr(number_style),
    ],
  }
  nodes[Paxos.client3.address] = {
    actor: Paxos.client3,
    svgs: [
      snap.circle(50, 250, 20).attr(colored(flat_red)),
      snap.text(50, 252, '3').attr(number_style),
    ],
  }

  // Proposers.
  nodes[Paxos.proposer1.address] = {
    actor: Paxos.proposer1,
    svgs: [
      snap.circle(200, 100, 20).attr(colored(flat_blue)),
      snap.text(200, 102, '1').attr(number_style),
    ],
  }
  nodes[Paxos.proposer2.address] = {
    actor: Paxos.proposer2,
    svgs: [
      snap.circle(200, 200, 20).attr(colored(flat_blue)),
      snap.text(200, 202, '2').attr(number_style),
    ],
  }

  // Acceptors.
  nodes[Paxos.acceptor1.address] = {
    actor: Paxos.acceptor1,
    svgs: [
      snap.circle(350, 50, 20).attr(colored(flat_green)),
      snap.text(350, 52, '1').attr(number_style),
    ],
  }
  nodes[Paxos.acceptor2.address] = {
    actor: Paxos.acceptor2,
    svgs: [
      snap.circle(350, 150, 20).attr(colored(flat_green)),
      snap.text(350, 152, '2').attr(number_style),
    ],
  }
  nodes[Paxos.acceptor3.address] = {
    actor: Paxos.acceptor3,
    svgs: [
      snap.circle(350, 250, 20).attr(colored(flat_green)),
      snap.text(350, 252, '3').attr(number_style),
    ],
  }

  // Node titles.
  snap.text(50, 15, 'Clients').attr({'text-anchor': 'middle'});
  snap.text(200, 15, 'Proposers').attr({'text-anchor': 'middle'});
  snap.text(350, 15, 'Acceptors').attr({'text-anchor': 'middle'});

  return nodes
}

function make_app(Paxos, snap, app_id) {
  let nodes = make_nodes(Paxos, snap);

  // Create the vue app.
  let vue_app = new Vue({
    el: app_id,

    components: {
      'abbreviated-acceptor-info': abbreviated_acceptor_info,
    },

    data: {
      acceptor1: nodes[Paxos.acceptor1.address],
      acceptor2: nodes[Paxos.acceptor2.address],
      acceptor3: nodes[Paxos.acceptor3.address],
      node: nodes[Paxos.client1.address],
      transport: Paxos.transport,
      send_message: (message, callback) => {
        let src = nodes[message.src];
        let dst = nodes[message.dst];
        let svg_message =
          snap.circle(src.svgs[0].attr("cx"), src.svgs[0].attr("cy"), 9)
              .attr({fill: '#2c3e50'});
        snap.prepend(svg_message);
        svg_message.animate(
          {cx: dst.svgs[0].attr("cx"), cy: dst.svgs[0].attr("cy")},
          250 + Math.random() * 200,
          callback);
      }
    },

    computed: {
      current_component: function() {
        if (this.node.actor.address.address.includes('Client')) {
          return client_info;
        } else if (this.node.actor.address.address.includes('Proposer')) {
          // return proposer_box;
        } else if (this.node.actor.address.address.includes('Acceptor')) {
          return acceptor_info;
        } else {
          // Impossible!
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

function clickthrough_app() {
  let Paxos = zeno.examples.js.ClickthroughPaxos.Paxos;
  let snap = Snap('#clickthrough_animation');

  // Create nodes.
  let nodes = {};
  nodes[Paxos.server.address] = {
    actor: Paxos.server,
    svg: snap.circle(150, 50, 20).attr(
        {fill: '#e74c3c', stroke: 'black', 'stroke-width': '3pt'}),
  };
  nodes[Paxos.clientA.address] = {
    actor: Paxos.clientA,
    svg: snap.circle(75, 150, 20).attr(
        {fill: '#3498db', stroke: 'black', 'stroke-width': '3pt'}),
  };
  nodes[Paxos.clientB.address] = {
    actor: Paxos.clientB,
    svg: snap.circle(225, 150, 20).attr(
        {fill: '#2ecc71', stroke: 'black', 'stroke-width': '3pt'}),
  };

  // Add node titles.
  snap.text(150, 20, 'Server').attr({'text-anchor': 'middle'});
  snap.text(75, 190, 'Client A').attr({'text-anchor': 'middle'});
  snap.text(225, 190, 'Client B').attr({'text-anchor': 'middle'});

  // Create the vue app.
  let vue_app = new Vue({
    el: '#clickthrough_app',

    data: {
      node: nodes[Paxos.client1.address],
      transport: Paxos.transport,
      send_message: (message, callback) => {
        let src = nodes[message.src];
        let dst = nodes[message.dst];
        let svg_message =
          snap.circle(src.svg.attr("cx"), src.svg.attr("cy"), 9)
              .attr({fill: '#2c3e50'});
        snap.prepend(svg_message);
        svg_message.animate(
          {cx: dst.svg.attr("cx"), cy: dst.svg.attr("cy")},
          200,
          callback);
      }
    },

    computed: {
      current_component: function() {
        if (this.node.actor != Paxos.server) {
          return client_paxos_box;
        }
      },
    },
  });

  // Select a node by clicking it.
  for (let node of Object.values(nodes)) {
    node.svg.node.onclick = () => {
      vue_app.node = node;
    }
  }
}

function main() {
  make_app(zeno.examples.js.SimulatedPaxos.Paxos,
           Snap('#simulated_animation'),
           '#simulated_app');

  make_app(zeno.examples.js.ClickthroughPaxos.Paxos,
           Snap('#clickthrough_animation'),
           '#clickthrough_app');
}

window.onload = main
