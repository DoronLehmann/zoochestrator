[![Build Status](https://www.travis-ci.org/imperva/zoochestrator.svg?branch=travis)](https://www.travis-ci.org/imperva/zoochestrator?branch=travis)

# ZooChestrator
ZooChestrator is a library for managing task distribution between several computer nodes.

The library is built on top of [Apache ZooKeeper](https://zookeeper.apache.org/) and [Apache Curator](https://curator.apache.org/), and uses some of their recipes in order to manage the process, such as Leadership Election, Locks, Latches, and nodes. This library implements and extends the ZooKeeper “master-worker” pattern, which is widely described in Oreilly’s book ["ZooKeeper -  Distributed Process Coordination”](http://shop.oreilly.com/product/0636920028901.do), by [Flavio Junqueira](https://twitter.com/fpjunqueira) and [Benjamin Reed](https://www.oreilly.com/pub/au/5985).
The book sample code can be found at this [GitHub repository](https://github.com/fpj/zookeeper-book-example).


# Benefits
Working with ZooKeeper is not a trivial programming task.

To properly build a system which will work with ZooKeeper requires deep understanding of its architecture and knowledge of its many edge cases and scenarios. Our goal was to design a library which will assist developers of distributed systems to manage tasks across server nodes with a light and easy interface.
By doing so, we are internally handling the entire process of connecting to the ZooKeeper cluster, handling connectivity issues, leader elections, failovers, recovery, etc. The developer can thus focus on its main business logic and can adopt the “master-workers” distributed task workflow without deep understanding of ZooKeeper internals.

# Dependencies

- Apache Curator 2.12.0
- Google Gson 2.2





# High Level Overview
 
In order to implement the “master-workers” workflow, the following zNodes structure is created in ZooKeeper:
![](/images/1.png)


The first zNode which is created under the root zNode and represents the “namespace” or the scope. This enables a complete separation between different scopes workflows.

Under each namespace, four zNodes are created:

* **master**

    Holds ephemeral zNodes of server nodes which are registered as candidates for gaining leadership. The leader election logic is based on the zNodes which are created under the “master” zNode.

* **workers**

    Holds ephemeral zNodes of server nodes which are registered for receiving tasks for execution. When a task is created, a task distribution algorithm selects which worker node the task will be assigned, according to the available nodes under the “workers” zNode. Since the zNodes are ephemeral, it will only select a server node which is alive and connected. 

* **assign**

    A persistent zNode is created under the “assign” zNode for every worker node. The assigned worker zNode holds its assigned tasks. 

* **tasks**

    Tasks for execution are created as persistent zNodes under the “tasks” zNode. An event is triggered whenever a task zNode is created, and the task is assigned to a worker node. When a task is assigned, it is deleted from the “tasks” zNode, and created as a zNode under the assigned worker zNode.


Here is an example state structure of an application with two server nodes, which are both registered for leadership:

![](/images/2.png)
 
As you can see, nodes A and node B are created under the “master zNode”, which implies that they are both candidates for leadership, and also under the “workers” zNode, which states that they are both eligible for getting tasks.

Under the “assign” zNode, we have node A which holds tasks 2 & 4, and node B which holds tasks 1, 3 and 5. The “tasks” zNode does not hold any tasks which pends distribution at the moment, since the task assignment is done immediately once a task is created under the “tasks” zNode.

Now, let’s review a scenario where node B has stopped working. In such a case, we identify this incident when the relevant event is triggered when node B’s ephemeral nodes are deleted. When the ZooChestrator identifies that node B zNode under the “workers” zNode is deleted, it starts running its task recovery logic. The task recovery logic takes all tasks node which are under the relevant assigned worker zNode, and moves them under the “tasks” zNode:

![](/images/3.png)

Once the tasks are created under the “tasks” zNode, the tasks distribution algorithm assigns the tasks to the available worker nodes, which is node A in our scenario:

![](/images/4.png)


# How to use it

The main entry point for using ZooChestrator is the `TasksSyncManager` class. When the `TasksSyncManager` is initialized, it gets all the relevant ZooKeeper connectivity details such as IPs, credentials, etc.

Once it is initialized, register to the manager for a selected scope in order to be eligible for handling tasks. Note that you can register to multiple scopes, which enables a single application to handle distributed tasks for various use cases.  

When registering, you can state if you would like to be nominated to become a master, or simply a worker node. In addition, you can provide the tasks distribution algorithm which is used in order to distribute tasks between the nodes. 

The default algorithm is `RandomTaskDistributionAlgorithm`, and the source code also provides two additional out-of-the-box algorithms - `RoundRobinDistributionAlgorithm` and `MachineLoadTaskDistributionAlgorithm`. Note that this can be extended by implementing the `TaskDistributedAlgorithm` interface.

Perhaps the most important part when registering to the `TasksSyncManager` is the `TaskAssignmentCallback` which you should provide. This callback will be triggered whenever a task is assigned to your working node, and that is where the node logic should be implemented.

Here is a quick example of the above:


```Java
// Creating the TasksSyncManager
final TasksSyncManager tasksSyncManager = new TasksSyncManager();
// Initializing
tasksSyncManager.init( zkServersIps, "my.server.1", "Doron", "testUser", "testPassword", false );
// Registering to the “cats” scope with a RoundRobinDistributionAlgorithm
tasksSyncManager.register( "/cats", true, new RoundRobinDistributionAlgorithm(), new TaskAssignmentCallback() {
    @Override
    public void execute( Task task ) {
        // On task assignment - we print the task path and delete the task
        System.out.println( "I got a new task - " + task.event.getData().getPath() );
        try {
            //Performing some business logic with the task data and finally deleting the task
            tasksSyncManager.deleteTask( task );
        }
        catch ( Exception e ) {
            logger.error( e.getMessage() );
        }
    }
} );
```

When your application node shuts down, it is highly recommended that you call the `TasksSyncManager.close() method`, which gracefully shuts down the manager and closes all of its connections.

# Tasks creation and handling 

New task creation is easy. However, in some use cases you would like to make sure that only the leader creates tasks. In order to do so, just so the following:

```Java
boolean hasLeadership = tasksSyncManager.hasLeadership( "/cats" );
if( hasLeadership ) {
    Task task = new Task( "{'catId': 5707, 'catName': 'Garfield'}", null );
    tasksSyncManager.createTask( "/cats", task  );
}
```
 
When the `TaskAssignmentCallback` is triggered, you get the Task object and can fetch its data in the following manner:

```Java
String taskDataJson = task.data;
```

Simple, right? :-)

Keep in mind that once a node gets the task, it is now responsible for the task lifecycle. I.e. it should run its logic, and then decide what should be done next - for example - delete the task, or delete it and create a new one instead, etc.

You can store a task data in the task `data` field, as shown in the example above, and in addition you could also use the task metadata field - which is added to the task path. This is a handy optimization for preventing unnecessary calls for getting the data of the task, when all is needed is just a simple metadata level information (for example - ID, group, etc.)

Here is an example of all of the above use cases:

```Java
public static void main( String[] args ) throws Exception {

    final String id = "my.server.1";
    final String scope = "/cats";
    List<String> zkServersIps = new ArrayList<>();
    zkServersIps.add( "192.168.0.1" );
    
    // Create and initialize the tasksSyncManager
    final TasksSyncManager tasksSyncManager = new TasksSyncManager();
    tasksSyncManager.init( zkServersIps, id, "Doron", "testUser", "testPassword", false );
    
    // define the callback logic
    TaskAssignmentCallback callback = new TaskAssignmentCallback() {
        @Override
        public void execute( Task task ) {
            System.out.println( "I got a new task - " + task.event.getData().getPath() );
            try {
                // create a new task based on the received task information
                Task newTask = new Task( task.data.substring( task.data.lastIndexOf( "-" ) + 1 ) + "-" + System.currentTimeMillis(), null );
                // get the account ID from the task metadata
                String metadataItemFromTask = Task.getMetadataItemFromTask( task.event.getData().getPath(), "accountId" );
                Long accountIdFromTask = 0L;
                if ( metadataItemFromTask != null ) {
                    accountIdFromTask = Long.valueOf( metadataItemFromTask );
                }
                Map<String, String> metaMap = new HashMap<>();
                metaMap.put( "accountId", String.valueOf( accountIdFromTask + 1 ) );
                // add metadata to the new task
                newTask.setMetadata( metaMap );
                // delete the current task and create a new task
                tasksSyncManager.deleteAndCreateFollowingTask( scope, task, newTask );
            }
            catch ( Exception e ) {
                logger.error( e.getMessage() );
            }
        }
    };
    
    // register to the task sync manager
    tasksSyncManager.register( scope, true, new RoundRobinDistributionAlgorithm(), callback );
    
    // create the first task for the scope
    Task firstTask = new Task( id, null );
    Map<String, String> metaMap = new HashMap<>();
    metaMap.put( "accountId", String.valueOf( 1 ) );
    firstTask.setMetadata( metaMap );
    tasksSyncManager.createTask( scope, firstTask );
    System.out.println( "Taking a 10 second sleep...Zzz..." );
    Thread.sleep( 10000 );
    System.out.println( "I'm up!" );
    // close the task sync manager
    tasksSyncManager.close();
}
```

# Getting Help

If you have questions about the library, please be sure to check out the source code documentation. 

If you still have questions, contact me at doron.lehmann@imperva.com

# Reporting Bugs

Open a Git Issue and include as much information as possible. If possible, provide sample code that illustrates the problem you're seeing.
Please do not open a Git Issue for help Git issues are reserved for bug reports only.


