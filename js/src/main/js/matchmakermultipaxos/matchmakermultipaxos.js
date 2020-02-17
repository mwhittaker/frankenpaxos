// Helper components ///////////////////////////////////////////////////////////
const phase1b_slot_info = {
  props: {
    value: Object,
  },

  template: `
    <fp-object>
      <fp-field :name="'slot'">
        {{value.slot}}
      </fp-field>
      <fp-field :name="'voteRound'">
        {{value.voteRound}}
      </fp-field>
      <fp-field :name="'voteValue'">
        {{value.voteValue}}
      </fp-field>
    </fp-object>
  `,
}

const phase1b_component = {
  props: {
    value: Object,
  },

  components: {
    'phase1b-slot-info': phase1b_slot_info,
  },

  template: `
    <fp-object>
      <fp-field :name="'groupIndex'">
        {{value.groupIndex}}
      </fp-field>
      <fp-field :name="'acceptorIndex'">
        {{value.acceptorIndex}}
      </fp-field>
      <fp-field :name="'round'">
        {{value.round}}
      </fp-field>
      <fp-field :name="'round'">
        <frankenpaxos-seq :seq="value.info">
          <phase1b-slot-info :value="value.info">
          </phase1b-slot-info>
        </frankenpaxos-seq>
      </fp-field>
    </fp-object>
  `,
}

const matchmaker_configuration_component = {
  props: {
    value: Object,
  },

  template: `
    <fp-object>
      <fp-field :name="'epoch'">
        {{value.epoch}}
      </fp-field>
      <fp-field :name="'matchmakerIndex'">
        {{value.matchmakerIndex}}
      </fp-field>
    </fp-object>
  `,
}

const matchmaking_component = {
  props: {
    value: Object,
  },

  components: {
    'matchmaker-configuration': matchmaker_configuration_component,
  },

  template: `
    <fp-object :value="value" v-slot="{let: state}">
      <fp-field :name="'round'">
        {{state.round}}
      </fp-field>
      <fp-field :name="'matchmakerConfiguration'">
        <matchmaker-configuration :value="state.matchmakerConfiguration">
        </matchmaker-configuration>
      </fp-field>
      <fp-field :name="'quorumSystem'">{{state.quorumSystem}}</fp-field>
      <fp-field :name="'quorumSystemProto'">
        {{state.quorumSystemProto}}
      </fp-field>
      <fp-field :name="'matchReplies'">
        <frankenpaxos-map :map="state.matchReplies">
        </frankenpaxos-map>
      </fp-field>
      <fp-field :name="'pendingClientRequests'">
        <frankenpaxos-horizontal-seq :seq="state.pendingClientRequests">
        </frankenpaxos-horizontal-seq>
      </fp-field>
      <fp-field :name="'resendMatchRequests'">
        {{state.resendMatchRequests}}
      </fp-field>
    </fp-object>
  `
}

const phase1_component = {
  props: {
    value: Object,
  },

  template: `
    <fp-object :value="value" v-slot="{let: state}">
      <fp-field :name="'round'">{{state.round}}</fp-field>
      <fp-field :name="'quorumSystem'">{{state.quorumSystem}}</fp-field>
      <fp-field :name="'previousQuorumSystems'">
        <frankenpaxos-map :map="state.previousQuorumSystems">
        </frankenpaxos-map>
      </fp-field>
      <fp-field :name="'acceptorToRounds'">
        <frankenpaxos-map :map="state.acceptorToRounds">
        </frankenpaxos-map>
      </fp-field>
      <fp-field :name="'pendingRounds'">
        {{state.pendingRounds}}
      </fp-field>
      <fp-field :name="'phase1bs'">
        <frankenpaxos-map :map="state.phase1bs">
        </frankenpaxos-map>
      </fp-field>
      <fp-field :name="'pendingClientRequests'">
        <frankenpaxos-horizontal-seq :seq="state.pendingClientRequests">
        </frankenpaxos-horizontal-seq>
      </fp-field>
      <fp-field :name="'resendPhase1as'">
        {{state.resendPhase1as}}
      </fp-field>
    </fp-object>
  `
}

const phase2_component = {
  props: {
    value: Object,
  },

  components: {
    'matchmaker-configuration': matchmaker_configuration_component,
  },

  template: `
    <fp-object :value="value" v-slot="{let: state}">
      <fp-field :name="'round'">{{state.round}}</fp-field>
      <fp-field :name="'nextSlot'">{{state.nextSlot}}</fp-field>
      <fp-field :name="'quorumSystem'">{{state.quorumSystem}}</fp-field>
      <fp-field :name="'values'">
        <frankenpaxos-map :map="state.values">
        </frankenpaxos-map>
      </fp-field>
      <fp-field :name="'phase2bs'">
        <frankenpaxos-map :map="state.phase2bs" v-slot="{value: v}">
          <frankenpaxos-map :map="v">
          </frankenpaxos-map>
        </frankenpaxos-map>
      </fp-field>
      <fp-field :name="'chosen'">
        <frankenpaxos-set :set="state.chosen">
        </frankenpaxos-set>
      </fp-field>
      <fp-field :name="'numChosenSinceLastWatermarkSend'">
        {{state.numChosenSinceLastWatermarkSend}}
      </fp-field>
      <fp-field :name="'resendPhase2as'">
        {{state.resendPhase2as}}
      </fp-field>
      <fp-field :name="'gc'">
        <div v-if="state.gc.constructor.name.includes('QueryingReplicas')">
          QueryingReplicas
          <fp-object :value="state.gc" v-slot="{let: gc}">
            <fp-field :name="'chosenWatermark'">
              {{gc.chosenWatermark}}
            </fp-field>
            <fp-field :name="'maxSlot'">
              {{gc.maxSlot}}
            </fp-field>
            <fp-field :name="'executedWatermarkReplies'">
              {{gc.executedWatermarkReplies}}
            </fp-field>
            <fp-field :name="'resendExecutedWatermarkRequests'">
              {{gc.resendExecutedWatermarkRequests}}
            </fp-field>
          </fp-object>
        </div>

        <div v-if="state.gc.constructor.name.includes('PushingToAcceptors')">
          PushingToAcceptors
          <fp-object :value="state.gc" v-slot="{let: gc}">
            <fp-field :name="'chosenWatermark'">
              {{gc.chosenWatermark}}
            </fp-field>
            <fp-field :name="'maxSlot'">
              {{gc.maxSlot}}
            </fp-field>
            <fp-field :name="'quorumSystem'">
              {{gc.quorumSystem}}
            </fp-field>
            <fp-field :name="'persistedAcks'">
              {{gc.persistedAcks}}
            </fp-field>
            <fp-field :name="'resendPersisted'">
              {{gc.resendPersisted}}
            </fp-field>
          </fp-object>
        </div>

        <div v-if="state.gc.constructor.name.includes('WaitingForLargerChosenWatermark')">
          WaitingForLargerChosenWatermark
          <fp-object :value="state.gc" v-slot="{let: gc}">
            <fp-field :name="'chosenWatermark'">
              {{gc.chosenWatermark}}
            </fp-field>
            <fp-field :name="'maxSlot'">
              {{gc.maxSlot}}
            </fp-field>
          </fp-object>
        </div>

        <div v-if="state.gc.constructor.name.includes('GarbageCollecting')">
          GarbageCollecting
          <fp-object :value="state.gc" v-slot="{let: gc}">
            <fp-field :name="'gcWatermark'">
              {{gc.gcWatermark}}
            </fp-field>
            <fp-field :name="'matchmakerConfiguration'">
              <matchmaker-configuration :value="gc.matchmakerConfiguration">
              </matchmaker-configuration>
            </fp-field>
            <fp-field :name="'garbageCollectAcks'">
              {{gc.garbageCollectAcks}}
            </fp-field>
            <fp-field :name="'resendGarbageCollects'">
              {{gc.resendGarbageCollects}}
            </fp-field>
          </fp-object>
        </div>

        <div v-if="state.gc.constructor.name.includes('Done')">
          Done
        </div>

        <div v-if="state.gc.constructor.name.includes('Cancelled')">
          Cancelled
        </div>
      </fp-field>
    </fp-object>
  `
}

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
      this.node.actor.propose(0, this.proposal);
      this.proposal = "";
    },

    propose_ten: function() {
      if (this.proposal === "") {
        return;
      }
      for (let i = 0; i < 10; ++i) {
        this.node.actor.propose(i, this.proposal);
      }
      this.proposal = "";
    },
  },

  template: `
    <div>
      <div>
        round = {{node.actor.round}}
      </div>

      <div>
        ids =
        <frankenpaxos-map :map="node.actor.ids">
        </frankenpaxos-map>
      </div>

      <div>
        pendingCommands =
        <frankenpaxos-map
          :map="node.actor.pendingCommands"
          v-slot="{value: pc}">
          <fp-object>
            <fp-field :name="'pseudonym'">{{pc.pseudonym}}</fp-field>
            <fp-field :name="'id'">{{pc.id}}</fp-field>
            <fp-field :name="'command'">{{pc.command}}</fp-field>
            <fp-field :name="'result'">{{pc.result}}</fp-field>
          </fp-object>
        </frankenpaxos-map>
      </div>

      <button v-on:click="propose">Propose</button>
      <button v-on:click="propose_ten">Propose Ten</button>
      <input v-model="proposal" v-on:keyup.enter="propose"></input>
    </div>
  `,
};

const leader_info = {
  props: {
    node: Object,
  },

  components: {
    'phase1b': phase1b_component,
    'matchmaker-configuration': matchmaker_configuration_component,
    'matchmaking': matchmaking_component,
    'phase1': phase1_component,
    'phase2': phase2_component,
  },

  template: `
    <div>
      <button v-on:click="node.actor.reconfigure()">Reconfigure</button>

      <div>
        chosenWatermark = {{node.actor.chosenWatermark}}
      </div>

      <div>
        matchmakerConfiguration =
        <matchmaker-configuration :value="node.actor.matchmakerConfiguration">
        </matchmaker-configuration>
      </div>

      <div>
        state =
        <div v-if="node.actor.state.constructor.name.endsWith('Inactive')">
          Inactive
          <fp-object :value="node.actor.state" v-slot="{let: state}">
            <fp-field :name="'round'">
            {{state.round}}
            </fp-field>
          </fp-object>
        </div>

        <div v-if="node.actor.state.constructor.name.endsWith('Matchmaking') &&
                   !node.actor.state.constructor.name.includes('Phase2')">
          Matchmaking
          <matchmaking :value="node.actor.state"></matchmaking>
        </div>

        <div v-if="node.actor.state.constructor.name.endsWith('WaitingForNewMatchmakers')">
          WaitingForNewMatchmakers
          <fp-object :value="node.actor.state" v-slot="{let: state}">
            <fp-field :name="'round'">
              {{state.round}}
            </fp-field>
            <fp-field :name="'matchmakerConfiguration'">
              <matchmaker-configuration :value="state.matchmakerConfiguration">
              </matchmaker-configuration>
            </fp-field>
            <fp-field :name="'quorumSystem'">{{state.quorumSystem}}</fp-field>
            <fp-field :name="'quorumSystemProto'">
              {{state.quorumSystemProto}}
            </fp-field>
            <fp-field :name="'pendingClientRequests'">
              <frankenpaxos-horizontal-seq :seq="state.pendingClientRequests">
              </frankenpaxos-horizontal-seq>
            </fp-field>
            <fp-field :name="'resendReconfigure'">
              {{state.resendReconfigure}}
            </fp-field>
          </fp-object>
        </div>

        <div v-if="node.actor.state.constructor.name.endsWith('Phase1')">
          Phase1
          <phase1 :value="node.actor.state"></phase1>
        </div>

        <div v-if="node.actor.state.constructor.name.endsWith('Phase2')">
          Phase2
          <phase2 :value="node.actor.state"></phase2>
        </div>

        <div v-if="node.actor.state.constructor.name.endsWith('Phase2Matchmaking')">
          Phase2Matchmaking
          <fp-object :value="node.actor.state" v-slot="{let: state}">
            <fp-field :name="'phase2'">
              <phase2 :value="state.phase2"></phase2>
            </fp-field>
            <fp-field :name="'matchmaking'">
              <matchmaking :value="state.matchmaking"></matchmaking>
            </fp-field>
          </fp-object>
        </div>

        <div v-if="node.actor.state.constructor.name.endsWith('Phase212')">
          Phase212
          <fp-object :value="node.actor.state" v-slot="{let: state}">
            <fp-field :name="'oldPhase2'">
              <phase2 :value="state.oldPhase2"></phase2>
            </fp-field>
            <fp-field :name="'newPhase1'">
              <phase1 :value="state.newPhase1"></phase1>
            </fp-field>
            <fp-field :name="'newPhase2'">
              <phase2 :value="state.newPhase2"></phase2>
            </fp-field>
          </fp-object>
        </div>

        <div v-if="node.actor.state.constructor.name.endsWith('Phase22')">
          Phase22
          <fp-object :value="node.actor.state" v-slot="{let: state}">
            <fp-field :name="'oldPhase2'">
              <phase2 :value="state.oldPhase2"></phase2>
            </fp-field>
            <fp-field :name="'newPhase2'">
              <phase2 :value="state.newPhase2"></phase2>
            </fp-field>
          </fp-object>
        </div>
      </div>
    </div>
  `,
};

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

const reconfigurer_info = {
  props: {
    node: Object,
  },

  data: function() {
    return {
      m1: "",
      m2: "",
      m3: "",
    };
  },

  methods: {
    reconfigure: function() {
      if (this.m1 === "" || this.m2 === "" || this.m3 === "" ) {
        return;
      }
      this.node.actor.reconfigureF1(parseInt(this.m1),
          parseInt(this.m2), parseInt(this.m3));
    },
  },

  components: {
    'matchmaker-configuration': matchmaker_configuration_component,
  },

  template: `
    <div>
      <button v-on:click="reconfigure">Reconfigure</button>
      <div>
        <input v-model="m1"></input>
        <input v-model="m2"></input>
        <input v-model="m3"></input>
      </div>

      state =
      <div v-if="node.actor.state.constructor.name.includes('Idle')">
        Idle
        <fp-object :value="node.actor.state" v-slot="{let: state}">
          <fp-field :name="'matchmakerConfiguration'">
            <matchmaker-configuration :value="state.configuration">
            </matchmaker-configuration>
          </fp-field>
        </fp-object>
      </div>

      <div v-if="node.actor.state.constructor.name.includes('Stopping')">
        Stopping
        <fp-object :value="node.actor.state" v-slot="{let: state}">
          <fp-field :name="'configuration'">
            <matchmaker-configuration :value="state.configuration">
            </matchmaker-configuration>
          </fp-field>
          <fp-field :name="'newConfiguration'">
            <matchmaker-configuration :value="state.newConfiguration">
            </matchmaker-configuration>
          </fp-field>
          <fp-field :name="'stopAcks'">
            <frankenpaxos-map :map="state.stopAcks">
            </frankenpaxos-map>
          </fp-field>
          <fp-field :name="'resendStops'">
            {{state.resendStops}}
          </fp-field>
        </fp-object>
      </div>

      <div v-if="node.actor.state.constructor.name.includes('Bootstrapping')">
        Bootstrapping
        <fp-object :value="node.actor.state" v-slot="{let: state}">
          <fp-field :name="'configuration'">
            <matchmaker-configuration :value="state.configuration">
            </matchmaker-configuration>
          </fp-field>
          <fp-field :name="'newConfiguration'">
            <matchmaker-configuration :value="state.newConfiguration">
            </matchmaker-configuration>
          </fp-field>
          <fp-field :name="'bootstrapAcks'">
            <frankenpaxos-map :map="state.bootstrapAcks">
            </frankenpaxos-map>
          </fp-field>
          <fp-field :name="'resendBootstraps'">
            {{state.resendBootstraps}}
          </fp-field>
        </fp-object>
      </div>

      <div v-if="node.actor.state.constructor.name.includes('Phase1')">
        Phase1
        <fp-object :value="node.actor.state" v-slot="{let: state}">
          <fp-field :name="'configuration'">
            <matchmaker-configuration :value="state.configuration">
            </matchmaker-configuration>
          </fp-field>
          <fp-field :name="'newConfiguration'">
            <matchmaker-configuration :value="state.newConfiguration">
            </matchmaker-configuration>
          </fp-field>
          <fp-field :name="'round'">
            {{state.round}}
          </fp-field>
          <fp-field :name="'matchPhase1bs'">
            <frankenpaxos-map :map="state.matchPhase1bs">
            </frankenpaxos-map>
          </fp-field>
          <fp-field :name="'resendMatchPhase1as'">
            {{state.resendMatchPhase1as}}
          </fp-field>
        </fp-object>
      </div>

      <div v-if="node.actor.state.constructor.name.includes('Phase2')">
        Phase2
        <fp-object :value="node.actor.state" v-slot="{let: state}">
          <fp-field :name="'configuration'">
            <matchmaker-configuration :value="state.configuration">
            </matchmaker-configuration>
          </fp-field>
          <fp-field :name="'newConfiguration'">
            <matchmaker-configuration :value="state.newConfiguration">
            </matchmaker-configuration>
          </fp-field>
          <fp-field :name="'round'">
            {{state.round}}
          </fp-field>
          <fp-field :name="'matchPhase2bs'">
            <frankenpaxos-map :map="state.matchPhase2bs">
            </frankenpaxos-map>
          </fp-field>
          <fp-field :name="'resendMatchPhase2as'">
            {{state.resendMatchPhase2as}}
          </fp-field>
        </fp-object>
      </div>
    </div>
  `,
};

const matchmaker_info = {
  props: {
    node: Object,
  },

  template: `
    <div>
      <div>
        matchmakerStates =
        <frankenpaxos-map
          :map="node.actor.matchmakerStates"
          v-slot="{value: state}">

          <div v-if="state.constructor.name.includes('Pending')">
            Pending
            <frankenpaxos-map :map="state.logs" v-slot="{value: log}">
              <fp-object>
                <fp-field :name="'gcWatermark'">
                  {{log.gcWatermark}}
                </fp-field>
                <fp-field :name="'reconfigureWatermark'">
                  {{log.reconfigureWatermark}}
                </fp-field>
                <fp-field :name="'configurations'">
                  <frankenpaxos-map :map="log.configurations">
                  </frankenpaxos-map>
                </fp-field>
              </fp-object>
            </frankenpaxos-map>
          </div>

          <div v-if="state.constructor.name.includes('Normal')">
            Normal
            <fp-object :value="state">
              <fp-field :name="'gcWatermark'">
                {{state.gcWatermark}}
              </fp-field>
              <fp-field :name="'reconfigureWatermark'">
                {{state.reconfigureWatermark}}
              </fp-field>
              <fp-field :name="'configurations'">
                <frankenpaxos-map :map="state.configurations">
                </frankenpaxos-map>
              </fp-field>
            </fp-object>
          </div>

          <div v-if="state.constructor.name.includes('HasStopped')">
            HasStopped
            <fp-object :value="state">
              <fp-field :name="'gcWatermark'">
                {{state.gcWatermark}}
              </fp-field>
              <fp-field :name="'reconfigureWatermark'">
                {{state.reconfigureWatermark}}
              </fp-field>
              <fp-field :name="'configurations'">
                <frankenpaxos-map :map="state.configurations">
                </frankenpaxos-map>
              </fp-field>
            </fp-object>
          </div>
        </frankenpaxos-map>
      </div>

      <div>
        acceptorStates =
        <frankenpaxos-map
          :map="node.actor.acceptorStates"
          v-slot="{value: state}">
          <fp-object>
            <fp-field :name="'round'">
              {{state.round}}
            </fp-field>
            <fp-field :name="'voteRound'">
              {{state.voteRound}}
            </fp-field>
            <fp-field :name="'voteValue'">
              {{state.voteValue}}
            </fp-field>
          </fp-object>
        </frankenpaxos-map>
      </div>
    </div>
  `,
};

const acceptor_info = {
  props: {
    node: Object,
  },

  template: `
    <div>
      <div>
        round = {{node.actor.round}}
      </div>

      <div>
        persistedWatermark = {{node.actor.persistedWatermark}}
      </div>

      <div>
        states =
        <frankenpaxos-map :map="node.actor.states" v-slot="{value: state}">
          <fp-object>
            <fp-field :name="'voteRound'">{{state.voteRound}}</fp-field>
            <fp-field :name="'voteValue'">{{state.voteValue}}</fp-field>
          </fp-object>
        </frankenpaxos-map>
      </div>
    </div>
  `,
};

const replica_info = {
  props: {
    node: Object,
  },

  template: `
    <div>
      <div>
        executedWatermark = {{node.actor.executedWatermark}}
      </div>

      <div>
        numChosen = {{node.actor.numChosen}}
      </div>

      <div>
        log =
        <frankenpaxos-buffer-map :value="node.actor.log">
        </frankenpaxos-buffer-map>
      </div>

      <div>
        clientTable =
        <frankenpaxos-map :map="node.actor.clientTable">
        </frankenpaxos-map>
      </div>
    </div>
  `,
}

// Main app ////////////////////////////////////////////////////////////////////
function make_nodes(MatchmakerMultiPaxos, snap) {
  // https://flatuicolors.com/palette/defo
  const flat_red = '#e74c3c';
  const flat_blue = '#3498db';
  const flat_orange = '#f39c12';
  const flat_green = '#2ecc71';
  const flat_purple = '#9b59b6';
  const flat_dark_blue = '#2c3e50';
  const flat_turquoise = '#1abc9c';

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

  const client_x = 100;
  const leader_x = 200;
  const reconfigurer_x = 300;
  const matchmaker_x = 400;
  const acceptor_x = 500;
  const replica_x = 600;

  const nodes = {};

  // Clients.
  const clients = [
    {client: MatchmakerMultiPaxos.client1, y: 250},
    {client: MatchmakerMultiPaxos.client2, y: 350},
    {client: MatchmakerMultiPaxos.client3, y: 450},
  ]
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

  // Leaders.
  const leaders = [
    {leader: MatchmakerMultiPaxos.leader1, y: 300, ey: 275},
    {leader: MatchmakerMultiPaxos.leader2, y: 400, ey: 425},
  ]
  for (const [index, {leader, y, ey}] of leaders.entries()) {
    const color = flat_orange;
    nodes[leader.address] = {
      actor: leader,
      color: color,
      component: leader_info,
      svgs: [
        snap.circle(leader_x, y, 20).attr(colored(color)),
        snap.text(leader_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
    nodes[leader.electionAddress] = {
      actor: leader.election,
      color: color,
      component: election_info,
      svgs: [
        snap.circle(leader_x + 25, ey, 10).attr(colored(color)),
      ],
    };
  }

  // Reconfigurers.
  const reconfigurers = [
    {reconfigurer: MatchmakerMultiPaxos.reconfigurer1, y: 300},
    {reconfigurer: MatchmakerMultiPaxos.reconfigurer2, y: 400},
  ]
  for (const [index, {reconfigurer, y}] of reconfigurers.entries()) {
    const color = flat_dark_blue;
    nodes[reconfigurer.address] = {
      actor: reconfigurer,
      color: color,
      component: reconfigurer_info,
      svgs: [
        snap.circle(reconfigurer_x, y, 20).attr(colored(color)),
        snap.text(reconfigurer_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Matchmakers.
  const matchmakers = [
    {matchmaker: MatchmakerMultiPaxos.matchmaker1, y: 100},
    {matchmaker: MatchmakerMultiPaxos.matchmaker2, y: 200},
    {matchmaker: MatchmakerMultiPaxos.matchmaker3, y: 300},
    {matchmaker: MatchmakerMultiPaxos.matchmaker4, y: 400},
    {matchmaker: MatchmakerMultiPaxos.matchmaker5, y: 500},
    {matchmaker: MatchmakerMultiPaxos.matchmaker6, y: 600},
  ]
  for (const [index, {matchmaker, y}] of matchmakers.entries()) {
    const color = flat_green;
    nodes[matchmaker.address] = {
      actor: matchmaker,
      color: color,
      component: matchmaker_info,
      svgs: [
        snap.circle(matchmaker_x, y, 20).attr(colored(color)),
        snap.text(matchmaker_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Acceptors.
  const acceptors = [
    {acceptor: MatchmakerMultiPaxos.acceptor1, y: 100},
    {acceptor: MatchmakerMultiPaxos.acceptor2, y: 200},
    {acceptor: MatchmakerMultiPaxos.acceptor3, y: 300},
    {acceptor: MatchmakerMultiPaxos.acceptor4, y: 400},
    {acceptor: MatchmakerMultiPaxos.acceptor5, y: 500},
    {acceptor: MatchmakerMultiPaxos.acceptor6, y: 600},
  ]
  for (const [index, {acceptor, y}] of acceptors.entries()) {
    const color = flat_purple;
    nodes[acceptor.address] = {
      actor: acceptor,
      color: color,
      component: acceptor_info,
      svgs: [
        snap.circle(acceptor_x, y, 20).attr(colored(color)),
        snap.text(acceptor_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Replicas.
  const replicas = [
    {replica: MatchmakerMultiPaxos.replica1, y: 250},
    {replica: MatchmakerMultiPaxos.replica2, y: 350},
    {replica: MatchmakerMultiPaxos.replica3, y: 450},
  ]
  for (const [index, {replica, y}] of replicas.entries()) {
    const color = flat_blue;
    nodes[replica.address] = {
      actor: replica,
      color: color,
      component: replica_info,
      svgs: [
        snap.circle(replica_x, y, 20).attr(colored(color)),
        snap.text(replica_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Node titles.
  const anchor_middle = (text) => text.attr({'text-anchor': 'middle'});
  anchor_middle(snap.text(client_x, 75, 'Clients'));
  anchor_middle(snap.text(leader_x, 50, 'Leaders'));
  anchor_middle(snap.text(matchmaker_x, 75, 'Matchmakers'));
  anchor_middle(snap.text(acceptor_x, 50, 'Acceptors'));
  anchor_middle(snap.text(replica_x, 75, 'Replicas'));

  return nodes;
}

function main() {
  const MatchmakerMultiPaxos =
    frankenpaxos.matchmakermultipaxos.MatchmakerMultiPaxos.MatchmakerMultiPaxos;
  const snap = Snap('#animation');
  const nodes = make_nodes(MatchmakerMultiPaxos, snap);

  // Create the vue app.
  let vue_app = new Vue({
    el: '#app',

    data: {
      nodes: nodes,
      node: nodes[MatchmakerMultiPaxos.client1.address],
      transport: MatchmakerMultiPaxos.transport,
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

window.onload = main
