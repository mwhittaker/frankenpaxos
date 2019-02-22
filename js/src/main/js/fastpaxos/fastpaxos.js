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
      <div>proposedValue: {{node.actor.proposedValue}}</div>
      <div>chosenValue: {{node.actor.chosenValue}}</div>
      <div>phase2bResponses: {{node.actor.phase2bResponses}}</div>
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
      <div>status = {{node.actor.status}}</div>
      <div>proposedValue = {{node.actor.proposedValue}}</div>
      <div>phase1bResponses = {{node.actor.phase1bResponses}}</div>
      <div>phase2bResponses = {{node.actor.phase2bResponses}}</div>
      <div>chosenValue = {{node.actor.chosenValue}}</div>
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


function make_nodes(FastPaxos, snap) {
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
  nodes[FastPaxos.client1.address] = {
    actor: FastPaxos.client1,
    svgs: [
      snap.circle(50, 100, 20).attr(colored(flat_red)),
      snap.text(50, 102, '1').attr(number_style),
    ],
  }
  nodes[FastPaxos.client2.address] = {
    actor: FastPaxos.client2,
    svgs: [
      snap.circle(50, 200, 20).attr(colored(flat_red)),
      snap.text(50, 202, '2').attr(number_style),
    ],
  }
  nodes[FastPaxos.client3.address] = {
    actor: FastPaxos.client3,
    svgs: [
      snap.circle(50, 300, 20).attr(colored(flat_red)),
      snap.text(50, 302, '3').attr(number_style),
    ],
  }

  // Leaders.
  nodes[FastPaxos.leader1.address] = {
    actor: FastPaxos.leader1,
    svgs: [
      snap.circle(200, 50, 20).attr(colored(flat_blue)),
      snap.text(200, 52, '1').attr(number_style),
    ],
  }
  nodes[FastPaxos.leader2.address] = {
    actor: FastPaxos.leader2,
    svgs: [
      snap.circle(200, 350, 20).attr(colored(flat_blue)),
      snap.text(200, 352, '2').attr(number_style),
    ],
  }

  // Acceptors.
  nodes[FastPaxos.acceptor1.address] = {
    actor: FastPaxos.acceptor1,
    svgs: [
      snap.circle(350, 100, 20).attr(colored(flat_green)),
      snap.text(350, 102, '1').attr(number_style),
    ],
  }
  nodes[FastPaxos.acceptor2.address] = {
    actor: FastPaxos.acceptor2,
    svgs: [
      snap.circle(350, 200, 20).attr(colored(flat_green)),
      snap.text(350, 202, '2').attr(number_style),
    ],
  }
  nodes[FastPaxos.acceptor3.address] = {
    actor: FastPaxos.acceptor3,
    svgs: [
      snap.circle(350, 300, 20).attr(colored(flat_green)),
      snap.text(350, 302, '3').attr(number_style),
    ],
  }

  // Node titles.
  snap.text(50, 15, 'Clients').attr({'text-anchor': 'middle'});
  snap.text(200, 15, 'Leaders').attr({'text-anchor': 'middle'});
  snap.text(350, 15, 'Acceptors').attr({'text-anchor': 'middle'});

  return nodes
}

function make_app(FastPaxos, snap, app_id) {
  let nodes = make_nodes(FastPaxos, snap);

  // Create the vue app.
  let vue_app = new Vue({
    el: app_id,

    components: {
      'abbreviated-acceptor-info': abbreviated_acceptor_info,
    },

    data: {
      acceptor1: nodes[FastPaxos.acceptor1.address],
      acceptor2: nodes[FastPaxos.acceptor2.address],
      acceptor3: nodes[FastPaxos.acceptor3.address],
      node: nodes[FastPaxos.client1.address],
      transport: FastPaxos.transport,
      send_message: (message, callback) => {
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
        if (this.node.actor.address.address.includes('Client')) {
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
  make_app(frankenpaxos.fastpaxos.SimulatedFastPaxos.FastPaxos,
           Snap('#simulated_animation'),
           '#simulated_app');

  make_app(frankenpaxos.fastpaxos.ClickthroughFastPaxos.FastPaxos,
           Snap('#clickthrough_animation'),
           '#clickthrough_app');
}

window.onload = main
