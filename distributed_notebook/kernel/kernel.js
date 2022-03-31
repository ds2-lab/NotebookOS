define([
  'base/js/namespace'
], function(
  Jupyter
) {
  return {onload: function(){
    console.info('Kernel specific javascript loaded');

    // do more things here, like define a codemirror mode
    if (!Jupyter.notebook.metadata.hasOwnProperty("persistent_id")) {
      Jupyter.notebook.metadata.persistent_id = generateUUID()
      Jupyter.notebook.save_notebook()
    }

    evalcode = "persistent_id=\"" + Jupyter.notebook.metadata.persistent_id + "\""
    if (Jupyter.notebook.metadata.hasOwnProperty("replica_id")) {
      evalcode += "\nreplica_id=\"" + Jupyter.notebook.metadata.replica_id + "\""
    }

    Jupyter.notebook.kernel.execute(evalcode)
  }}
});

function generateUUID() { // Public Domain/MIT
  var d = new Date().getTime();//Timestamp
  var d2 = ((typeof performance !== 'undefined') && performance.now && (performance.now()*1000)) || 0;//Time in microseconds since page-load or 0 if unsupported
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
      var r = Math.random() * 16;//random number between 0 and 16
      if(d > 0){//Use timestamp until depleted
          r = (d + r)%16 | 0;
          d = Math.floor(d/16);
      } else {//Use microseconds since page-load if supported
          r = (d2 + r)%16 | 0;
          d2 = Math.floor(d2/16);
      }
      return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16);
  });
}