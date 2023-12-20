import { JupyterFrontEnd, JupyterFrontEndPlugin } from '@jupyterlab/application';
import { INotebookTracker, INotebookModel , NotebookPanel } from '@jupyterlab/notebook';
import { Kernel, ISessionContext } from '@jupyterlab/services';
import { ISessionConnection } from '@jupyterlab/services/Session';
import { IChangedArgs } from '@jupyterlab/coreutils';
import { CodeCell, CodeCellModel } from '@jupyterlab/cells';
import { v4 as uuidv4 } from 'uuid';

/**
 * Initialization data for the distributedKernel extension.
 */
const plugin: JupyterFrontEndPlugin<void> = {
  id: 'distributedKernel:plugin',
  description: 'Provides an ID that is associated with a given IPYNB file. This enables the restoration of runtime states when the associated notebook is opened.',
  autoStart: true,
  activate: activate
};

function activate(
  app: JupyterFrontEnd,
  notebooks: INotebookTracker
): void {
  console.log('JupyterLab extension distributedKernel is activated!');

  // Do something when the notebook is added
  notebooks.widgetAdded.connect((sender: INotebookTracker, widget: NotebookPanel) => {
    // Access the notebook model
    const notebookModel: INotebookModel | null = widget.content?.model;

    if (notebookModel) {
      // Access the metadata
      const metadata = notebookModel.metadata;

      if (!('persistent_id' in metadata)) {
        metadata['persistent_id'] = uuidv4()
        notebookModel.metadata = metadata; 
  
        console.log('Updated Notebook Metadata:', notebookModel.metadata);

        const sessionContext = app.serviceManager.sessions.connectTo({ model: widget.session });

        let codeToExecute:string  = "persistent_id=\"" + metadata['persistent_id'] + "\""
        if ("replica_id" in metadata) {
          codeToExecute += "\nreplica_id=\"" + metadata["replica_id"] + "\""
        }

        executePythonCode(codeToExecute, sessionContext)
          .catch(error => console.error('Error executing code:', error));
      } else {
        console.log("Notebook already has persistent_id: ", metadata['persistent_id'])
      }
    }

    // Attach an event listener to the kernelChanged event
    widget.context.session.kernelChanged.connect((sender: ISessionConnection, args: IChangedArgs) => {
      if (args.newValue) {
        // Execute your code when the kernel is loaded
        executeOnKernelLoaded(args.newValue);
      }
    });
  });
}

function executeOnKernelLoaded(kernel: Kernel.IKernelConnection): void {
  // Your code to be executed when the kernel is loaded
  console.log('Kernel is loaded:', kernel.name);
  // Add your code here
}

function executePythonCode(
  code: string,
  sessionContext: ISessionContext
): Promise<void> {
  const kernel = sessionContext.session?.kernel;

  if (kernel) {
    // Create a new code cell model with the provided code
    const cellModel = new CodeCellModel({});

    // Set the code for the cell model
    cellModel.value.text = code;

    // Create a new code cell with the cell model
    const codeCell = new CodeCell({ model: cellModel });

    // Execute the code cell in the notebook's kernel
    return codeCell.execute(kernel).then(() => {
      // Handle the execution completion if needed
      console.log('Code execution completed.');
    });
  } else {
    return Promise.reject('Kernel not available.');
  }
}

export default plugin;
