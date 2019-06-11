let object_and_methods = {
  props: {
    name: String,
    type: String,
    object: Object,
    methods: null,
  },

  data: function() {
    return {
      arg1: "",
      arg2: "",
    };
  },

  template: `
    <div class="object">
      {{name}}: {{type}} =
      <slot :object="object">{{object}}</slot>

      <div v-for="m in methods">
        <div v-if="m.method.length == 0">
          <button v-on:click="m.method()">{{m.name}}</button>()
        </div>
        <div v-if="m.method.length == 1">
          <button v-on:click="m.method(arg1)">{{m.name}}</button>
          (<input v-model="arg1"></input>)
        </div>
        <div v-else-if="m.method.length == 2">
          <button v-on:click="m.method(arg1, arg2)">{{m.name}}</button>
          (<input v-model="arg1"></input>,
           <input v-model="arg2"></input>)
        </div>
      </div>
    </div>
  `
};


function main() {
  let VueTest = frankenpaxos.vue_test.TweenedVueTest.VueTest;

  // Create the vue app.
  let vue_app = new Vue({
    el: '#tweened_app',

    components: {
      'object-and-methods': object_and_methods,
    },

    data: {
      VueTest: VueTest,

      mutableStrings: {
        name: 'mutableStrings',
        type: 'mutable.Buffer[String]',
        object: VueTest.mutableStrings,
        methods: [
          {
            name: 'plusEqualsMutableStrings',
            method: (x) => VueTest.plusEqualsMutableStrings(x),
          },
          {
            name: 'clearMutableStrings',
            method: () => VueTest.clearMutableStrings(),
          },
        ],
      },

      mutableString: {
        name: 'mutableString',
        type: 'mutable.Map[String, String]',
        object: VueTest.mutableString,
        methods: [
          {
            name: 'directAddMutableString',
            method: (k, v) => VueTest.directAddMutableString(k, v),
          },
          {
            name: 'putMutableString',
            method: (k, v) => VueTest.putMutableString(k, v),
          },
          {
            name: 'updateMutableString',
            method: (k, v) => VueTest.updateMutableString(k, v),
          },
          {
            name: 'plusEqualMutableString',
            method: (k, v) => VueTest.plusEqualMutableString(k, v),
          },
          {
            name: 'reassignMutableString',
            method: (k, v) => VueTest.reassignMutableString(k, v),
          },
          {
            name: 'plusMutableString',
            method: (k, v) => VueTest.plusMutableString(k, v),
          },
          {
            name: 'removeMutableString',
            method: (k) => VueTest.removeMutableString(k),
          },
          {
            name: 'minusEqualsMutableString',
            method: (k) => VueTest.minusEqualsMutableString(k),
          },
          {
            name: 'minusMutableString',
            method: (k) => VueTest.minusMutableString(k),
          },
        ],
      },

      mutableCell: {
        name: 'mutableCell',
        type: 'mutable.Map[String, Cell]',
        object: VueTest.mutableCell,
        methods: [
          {
            name: 'directAddMutableCell',
            method: (k, v) => VueTest.directAddMutableCell(k, v),
          },
          {
            name: 'putMutableCell',
            method: (k, v) => VueTest.putMutableCell(k, v),
          },
          {
            name: 'updateMutableCell',
            method: (k, v) => VueTest.updateMutableCell(k, v),
          },
          {
            name: 'plusEqualMutableCell',
            method: (k, v) => VueTest.plusEqualMutableCell(k, v),
          },
          {
            name: 'reassignMutableCell',
            method: (k, v) => VueTest.reassignMutableCell(k, v),
          },
          {
            name: 'plusMutableCell',
            method: (k, v) => VueTest.plusMutableCell(k, v),
          },
          {
            name: 'mutateMutableCell',
            method: (k, v) => VueTest.mutateMutableCell(k, v),
          },
          {
            name: 'removeMutableCell',
            method: (k) => VueTest.removeMutableCell(k),
          },
          {
            name: 'minusEqualsMutableCell',
            method: (k) => VueTest.minusEqualsMutableCell(k),
          },
          {
            name: 'minusMutableCell',
            method: (k) => VueTest.minusMutableCell(k),
          },
        ],
      },
    },
  });
}

window.onload = main
