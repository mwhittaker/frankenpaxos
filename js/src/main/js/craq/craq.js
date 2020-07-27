// Node components /////////////////////////////////////////////////////////////
const client_info = {
  props: {
    node: Object,
  },

  data: function() {
    return {
      write_value: "",
    };
  },

  methods: {
    read: function() {
      this.node.actor.read(0, this.write_value_key);
    },

    read_ten: function() {
      for (let i = 0; i < 10; ++i) {
        this.node.actor.read(i, this.write_value_key);
      }
    },

    write: function() {
      if (this.write_value === "") {
        return;
      }
      this.node.actor.write(0, this.write_value_key, this.write_value);
      this.write_value = "";
    },

    write_ten: function() {
      if (this.write_value === "") {
        return;
      }
      for (let i = 0; i < 10; ++i) {
        this.node.actor.write(i, this.write_value);
      }
      this.write_value = "";
    },
  },

  template: `
    <div>
      <div>
        round = {{node.actor.round}}
      </div>

      <div>
        ids = <frankenpaxos-map :map="node.actor.ids"></frankenpaxos-map>
      </div>

      <div>
        largestSeenSlots =
        <frankenpaxos-map :map="node.actor.largestSeenSlots">
        </frankenpaxos-map>
      </div>

      <div>
        states =
        <frankenpaxos-map :map="node.actor.states" v-slot="{value: state}">
          <div v-if="state.constructor.name.includes('PendingWrite')">
            PendingWrite
            <fp-object>
              <fp-field :name="'pseudonym'">{{state.pseudonym}}</fp-field>
              <fp-field :name="'id'">{{state.id}}</fp-field>
              <fp-field :name="'command'">{{state.command}}</fp-field>
              <fp-field :name="'result'">{{state.result}}</fp-field>
              <fp-field :name="'resendClientRequest'">
                {{state.resendClientRequest}}
              </fp-field>
            </fp-object>
          </div>


          <div v-if="state.constructor.name.includes('PendingRead')">
            PendingRead
            <fp-object>
              <fp-field :name="'pseudonym'">{{state.pseudonym}}</fp-field>
              <fp-field :name="'id'">{{state.id}}</fp-field>
              <fp-field :name="'command'">{{state.command}}</fp-field>
              <fp-field :name="'result'">{{state.result}}</fp-field>
              <fp-field :name="'resendReadRequest'">
                {{state.resendReadRequest}}
              </fp-field>
            </fp-object>
          </div>
        </frankenpaxos-map>
      </div>

      <div>
        <button v-on:click="read">Linearizable Read</button>
        <button v-on:click="read_ten">Linearizable Read Ten</button>
      </div>
      <div>
        <button v-on:click="write">Write</button>
        <button v-on:click="write_ten">Write Ten</button>
        <input v-model="write_value" v-on:keyup.enter="write"></input>
        <input v-model="write_value_key" v-on:keyup.enter="key"></input>
      </div>
    </div>
  `,
}


const election_info = {
  props: {
    node: Object,
  },

  template: `
    <div>
      <div>
        round = {{node.actor.round}}
      </div>
      <div>
        leaderIndex = {{node.actor.leaderIndex}}
      </div>
      <div>
        state = {{node.actor.state}}
      </div>
    </div>
  `,
}

const chain_node_info = {
  props: {
    node: Object,
  },

  template: `
    <div>
      <div>
        clientTable =
        <frankenpaxos-map :map="node.actor.clientTable">
        </frankenpaxos-map>
      </div>
    </div>
  `,
}

// Main app ////////////////////////////////////////////////////////////////////
function distance(x1, y1, x2, y2) {
  const dx = x1 - x2;
  const dy = y1 - y2;
  return Math.sqrt(dx*dx + dy*dy);
}

function make_nodes(Craq, snap, batched) {
  // https://flatuicolors.com/palette/defo
  const flat_red = '#e74c3c';
  const flat_dark_blue = '#2c3e50';

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

  const client_x        = batch ? 100  : 100;
  const chain_node_x       = batch ? 900  : 850;

  const nodes = {};

  // Clients.
  const clients = [
    {client: Craq.client1, y: 100},
    {client: Craq.client2, y: 200},
    {client: Craq.client3, y: 300},
    {client: Craq.client4, y: 400},
  ]
  console.log(Craq.client1)
  for (const [index, {client, y}] of clients.entries()) {
    const color = flat_red;
    nodes[client.address] = {
      actor: client,
      color: color,
      component: client_info,
      svgs: [
        snap.circle(client_x, y, 20).attr(colored(color)),
        snap.text(client_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Chain Nodes.
  const chainNodes = [
    {chainNode: Craq.chainNode1, y: 200},
    {chainNode: Craq.chainNode2, y: 300},
    {chainNode: Craq.chainNode3, y: 400},
  ]
  for (const [index, {chainNode, y}] of chainNodes.entries()) {
    const color = flat_dark_blue;
    nodes[chainNode.address] = {
      actor: chainNode,
      color: color,
      component: chain_node_info,
      svgs: [
        snap.circle(chain_node_x, y, 20).attr(colored(color)),
        snap.text(chain_node_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Node titles.
  const anchor_middle = (text) => text.attr({'text-anchor': 'middle'});
  anchor_middle(snap.text(client_x, 50, 'Clients'));
  anchor_middle(snap.text(chain_node_x, 50, 'Replicas'));

  return nodes;
}

function unbatched() {
  const Craq = frankenpaxos.craq.Craq.Craq;
  const snap = Snap('#animation');
  const nodes = make_nodes(Craq, snap, batch=false);

  // Create the vue app.
  let vue_app = new Vue({
    el: '#app',

    data: {
      nodes: nodes,
      node: nodes[Craq.client1.address],
      transport: Craq.transport,
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
        let d = distance(src_x, src_y, dst_x, dst_y);
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
  for (const node of Object.values(nodes)) {
    for (const svg of node.svgs) {
      svg.node.onclick = () => {
        vue_app.node = node;
      }
    }
  }
}

function batched() {
  const Craq = frankenpaxos.craq.BatchedCraq.Craq;
  const snap = Snap('#batched_animation');
  const nodes = make_nodes(Craq, snap, batch=true);

  // Create the vue app.
  let vue_app = new Vue({
    el: '#batched_app',

    data: {
      nodes: nodes,
      node: nodes[Craq.client1.address],
      transport: Craq.transport,
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
        let d = distance(src_x, src_y, dst_x, dst_y);
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
  for (const node of Object.values(nodes)) {
    for (const svg of node.svgs) {
      svg.node.onclick = () => {
        vue_app.node = node;
      }
    }
  }
}

function main() {
  unbatched();
  batched();
}

window.onload = main
