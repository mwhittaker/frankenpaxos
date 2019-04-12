// let client_info = {
//   props: ['node'],
//
//   data: function() {
//     return {
//       message: "",
//     };
//   },
//
//   methods: {
//     echo: function() {
//       if (this.message === "") {
//         return;
//       }
//       this.node.actor.echo(this.message);
//       this.message = "";
//     }
//   },
//
//   template: `
//     <div>
//       <div>Number of messages received: {{node.actor.numMessagesReceived}}</div>
//       <button v-on:click="echo">Echo</button>
//       <input v-model="message" v-on:keyup.enter="echo"></input>
//     </div>
//   `,
// };
//
// let server_info = {
//   props: ['node'],
//
//   template: `
//     <div>
//       <div>Number of messages received: {{node.actor.numMessagesReceived}}</div>
//     </div>
//   `,
// };

Vue.component('frankenpaxos-tween-app', {
  props: [
    'transport',
    // (message, callback) -> ().
    'send_message',
  ],

  template: `
    <frankenpaxos-transport
      :transport="transport"
      :callbacks="callbacks">
    </frankenpaxos-transport>
  `,

  data: function() {
    return {
      timers: {},
      callbacks: {
        timer_started: (timer) => {
          // If we reset a timer, it toggles from not running to running very
          // quickly. When this happens, Vue does not always trigger an event
          // for the stopping and starting of the timer. It usually just
          // triggers an event for the starting. Thus, if we start a timer that
          // is already started, we should cancel it first.
          if ([timer.address, timer.name()] in this.timers) {
            clearTimeout(this.timers[[timer.address, timer.name()]]);
            delete this.timers[[timer.address, timer.name()]];
          }

          this.timers[[timer.address, timer.name()]] = setTimeout(() => {
            timer.run();
          }, timer.delayMilliseconds());
        },
        timer_stopped: (timer) => {
          if ([timer.address, timer.name()] in this.timers) {
            clearTimeout(this.timers[[timer.address, timer.name()]]);
            delete this.timers[[timer.address, timer.name()]];
          }
        },
        message_buffered: (message) => {
          this.send_message(message, () => {
            this.transport.stageMessage(message);
          });
        },
        message_staged: (message) => {
          this.transport.deliverMessage(message);
        },
      }
    }
  }
});


function make_nodes(Echo, snap) {
  // Create the nodes.
  let nodes = {};
  nodes[Echo.server.address] = {
    actor: Echo.server,
    svg: snap.circle(150, 50, 20).attr(
        {fill: '#e74c3c', stroke: 'black', 'stroke-width': '3pt'}),
  };
  nodes[Echo.clientA.address] = {
    actor: Echo.clientA,
    svg: snap.circle(75, 150, 20).attr(
        {fill: '#3498db', stroke: 'black', 'stroke-width': '3pt'}),
  };
  nodes[Echo.clientB.address] = {
    actor: Echo.clientB,
    svg: snap.circle(225, 150, 20).attr(
        {fill: '#2ecc71', stroke: 'black', 'stroke-width': '3pt'}),
  };

  // Add node titles.
  snap.text(150, 20, 'Server').attr({'text-anchor': 'middle'});
  snap.text(75, 190, 'Client A').attr({'text-anchor': 'middle'});
  snap.text(225, 190, 'Client B').attr({'text-anchor': 'middle'});

  return nodes;
}

function make_app(Echo, snap, app_id) {
  let nodes = make_nodes(Echo, snap)

  // Create the vue app.
  let vue_app = new Vue({
    el: app_id,

    data: {
      node: nodes[Echo.server.address],
      transport: Echo.transport,
      send_message: (message, callback) => {
        let src = nodes[message.src];
        let dst = nodes[message.dst];
        let svg_message =
          snap.circle(src.svg.attr("cx"), src.svg.attr("cy"), 9)
              .attr({fill: '#2c3e50'});
        snap.prepend(svg_message);
        svg_message.animate(
          {cx: dst.svg.attr("cx"), cy: dst.svg.attr("cy")},
          250 + Math.random() * 200,
          callback);
      }
    },

    computed: {
      current_component: function() {
        if (this.node.actor.address.address.includes("Server")) {
          return server_info;
        } else if (this.node.actor.address.address.includes("Client")) {
          return client_info;
        } else {
          // Impossible!
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
  // make_app(frankenpaxos.echo.SimulatedEcho.Echo,
  //          Snap('#simulated_animation'),
  //          '#simulated_app');
  //
  var vue_app = new Vue({
    el: '#tween_app',

    methods: {
      change_speed: function(x) {
        console.log('Setting time scale to ' + this.timeScale);
        this.timeline.timeScale(this.timeScale);
      }
    },

    data: function() {
      let vm = this;

      return {
        x: 1,

        timeline: new TimelineMax(),
        timeScale: 1,

        timers: {},

        transport: frankenpaxos.echo.SimulatedEcho.Echo.transport,
        callbacks: {
          timer_started: function(timer) {
            let key = [timer.address, timer.name()];
            Vue.set(vm.timers, key, timer.delayMilliseconds());

            let end = {
              onComplete: function() {
                timer.run();
                // vm.timeline.remove(this);
              },
            };
            end[key] = 0;
            let p = vm.timeline.time();
            vm.timeline.to(vm.timers, timer.delayMilliseconds() / 1000, end, p);
          },
          timer_stopped: (timer) => {
            console.log('timer_stopped');
          },
          message_buffered: (message) => {
            console.log('message_buffered');
          },
          message_staged: (message) => {
            console.log('message_staged');
            vm.transport.deliverMessage(message);
          },
        },
      };
    },
  });

  // Create the vue app.
  // let vue_app = new Vue({
  //   el: 'tween',
  //
  //   data: {
  //     node: nodes[Echo.server.address],
  //     transport: Echo.transport,
  //     send_message: (message, callback) => {
  //       let src = nodes[message.src];
  //       let dst = nodes[message.dst];
  //       let svg_message =
  //         snap.circle(src.svg.attr("cx"), src.svg.attr("cy"), 9)
  //             .attr({fill: '#2c3e50'});
  //       snap.prepend(svg_message);
  //       svg_message.animate(
  //         {cx: dst.svg.attr("cx"), cy: dst.svg.attr("cy")},
  //         250 + Math.random() * 200,
  //         callback);
  //     }
  //   },
  //
  //   computed: {
  //     current_component: function() {
  //       if (this.node.actor.address.address.includes("Server")) {
  //         return server_info;
  //       } else if (this.node.actor.address.address.includes("Client")) {
  //         return client_info;
  //       } else {
  //         // Impossible!
  //       }
  //     },
  //   },
  // });
}

window.onload = main
