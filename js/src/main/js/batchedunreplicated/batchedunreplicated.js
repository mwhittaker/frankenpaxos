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
      this.node.actor.propose(this.proposal);
      this.proposal = "";
    },

    propose_ten: function() {
      if (this.proposal === "") {
        return;
      }
      for (let i = 0; i < 10; ++i) {
        this.node.actor.propose(this.proposal);
      }
      this.proposal = "";
    },
  },

  template: `
    <div>
      <div>
        nextId = {{node.actor.nextId}}
      </div>

      <div>
        pendingCommands =
        <frankenpaxos-map
          :map="node.actor.pendingCommands"
          v-slot="{value: pc}">
          <fp-object>
            <fp-field :name="'commandId'" :value="pc.commandId"></fp-field>
            <fp-field :name="'command'" :value="pc.command"></fp-field>
            <fp-field :name="'result'" :value="pc.result"></fp-field>
          </fp-object>
        </frankenpaxos-map>
      </div>

      <button v-on:click="propose">Propose</button>
      <button v-on:click="propose_ten">Propose Ten</button>
      <input v-model="proposal" v-on:keyup.enter="propose"></input>
    </div>
  `,
};

const batcher_info = {
  props: {
    node: Object,
  },

  template: `
    <div>
      <div>
        growingBatch =
        <frankenpaxos-seq :seq="node.actor.growingBatch">
        </frankenpaxos-seq>
      </div>
    </div>
  `,
};

const server_info = {
  props: {
    node: Object,
  },

  template: `
    <div>
      <div>
        stateMachine =
        {{node.actor.stateMachine}}
      </div>
    </div>
  `,
};

const proxy_server_info = {
  props: {
    node: Object,
  },

  template: `
    <div>
      <div>
        clients =
        <frankenpaxos-map :map="node.actor.clients">
        </frankenpaxos-map>
      </div>
    </div>
  `,
};


// Main app ////////////////////////////////////////////////////////////////////
function make_nodes(BatchedUnreplicated, snap) {
  // https://flatuicolors.com/palette/defo
  const flat_red = '#e74c3c';
  const flat_blue = '#3498db';
  const flat_orange = '#f39c12';
  const flat_green = '#2ecc71';
  const client_color = flat_red;
  const batcher_color = flat_blue;
  const server_color = flat_orange;
  const proxy_server_color = flat_green;

  const colored = (color) => {
    return {
      'fill': color,
      'stroke': 'black', 'stroke-width': '3pt',
    }
  };

  const number_style = {
    'text-anchor': 'middle',
    'alignment-baseline': 'middle',
    'font-size': '20pt',
    'font-weight': 'bolder',
    'fill': 'black',
    'stroke': 'white',
    'stroke-width': '1px',
  }

  const nodes = {};

  // Clients.
  const clients = [
    {client: BatchedUnreplicated.client1, x: 100, y: 100},
    {client: BatchedUnreplicated.client2, x: 100, y: 200},
    {client: BatchedUnreplicated.client3, x: 100, y: 300},
  ]
  for (const [index, {client, x, y}] of clients.entries()) {
    nodes[client.address] = {
      actor: client,
      color: client_color,
      component: client_info,
      svgs: [
        snap.circle(x, y, 20).attr(colored(client_color)),
        snap.text(x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Batchers.
  const batchers = [
    {batcher: BatchedUnreplicated.batcher1, x: 200, y: 150},
    {batcher: BatchedUnreplicated.batcher2, x: 200, y: 250},
  ]
  for (const [index, {batcher, x, y}] of batchers.entries()) {
    nodes[batcher.address] = {
      actor: batcher,
      color: batcher_color,
      component: batcher_info,
      svgs: [
        snap.circle(x, y, 20).attr(colored(batcher_color)),
        snap.text(x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Server.
  nodes[BatchedUnreplicated.server.address] = {
    actor: BatchedUnreplicated.server,
    color: server_color,
    component: server_info,
    svgs: [
      snap.circle(300, 200, 20).attr(colored(server_color)),
      snap.text(300, 200, 's').attr(number_style),
    ],
  };

  // ProxyServers.
  const proxy_servers = [
    {proxy_server: BatchedUnreplicated.proxyServer1, x: 400, y: 150},
    {proxy_server: BatchedUnreplicated.proxyServer2, x: 400, y: 250},
  ]
  for (const [index, {proxy_server, x, y}] of proxy_servers.entries()) {
    nodes[proxy_server.address] = {
      actor: proxy_server,
      color: proxy_server_color,
      component: proxy_server_info,
      svgs: [
        snap.circle(x, y, 20).attr(colored(proxy_server_color)),
        snap.text(x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Node titles.
  const anchor_middle = (text) => text.attr({'text-anchor': 'middle'});
  anchor_middle(snap.text(100, 50, 'Clients'));
  anchor_middle(snap.text(200, 50, 'Batchers'));
  anchor_middle(snap.text(300, 50, 'Server'));
  anchor_middle(snap.text(400, 50, 'Proxy Servers'));

  return nodes;
}

function main() {
  const BatchedUnreplicated =
    frankenpaxos.batchedunreplicated.BatchedUnreplicated.BatchedUnreplicated;
  const snap = Snap('#animation');
  const nodes = make_nodes(BatchedUnreplicated, snap);

  // Create the vue app.
  let vue_app = new Vue({
    el: '#app',

    data: {
      nodes: nodes,
      node: nodes[BatchedUnreplicated.client1.address],
      transport: BatchedUnreplicated.transport,
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
        let speed = 100 + (Math.random() * 50); // px per second.

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
  for (const node of Object.values(nodes)) {
    for (const svg of node.svgs) {
      svg.node.onclick = () => {
        vue_app.node = node;
      }
    }
  }
}

window.onload = main
