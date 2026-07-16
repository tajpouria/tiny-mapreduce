# tiny-mapreduce

paper: https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf

the blueprint

master split input into M pieces, where M is number of map functions
master picks up idle worker (workers requesting tasks (pull-based)) and assign them map/reduce task
map worker:
reads the data from assigned split
parse it into (key, value) pairs
passes them to the user's map function
get the intermediate (key, value) list and buffer it
writes the intermediate (key, value) list into it's local disk in R partition where R is number of reduce task, using the formula hash(key) % R
reduce worker:
make RPC call to all of the map workers, asking for it's partition
sorts the data by key
to not load the entire intermediate data in memory for sorting it we can use "External Merge Sort" ? is there a go pkg to implement this? should I implement this?
iterates over sorted (key, values), streams them into user's reduce function
output written to as shared storage (one file per reduce task)
implementation

function reduce (k2, list(v2)) list(v2)

in the reduce, data needs to be streamed to the function, for example using a iterator, this is to save memory
all of the map tasks must be completed before any reduce starts
function map (k1, v1) list(k2, v2)

(optional) implement a "combiner function" in the map function that "reduces" the intermediate file (key, value) pairs to save network bandwidth. this is basically the same reduce function but in map
master

tracks the task state (idle, in-progress, completed)
which worker does which task
for completed map tasks stores location and size of the R intermediate files
(optional) if a reduce ask for reduce task, before all of the map tasks are completed, master must reply with "wait/sleep"
fault tolerance

master will:
pings workers (with a task) every interval seconds to simplify the networking, heartbeat can be also achieved by setting a timeout on worker task completing
if worker dies, reset it's task state to idle
idle task will be reassigned
avoiding zombie workers

for map task this is to avoid updating the location of the R intermediate file on master, to avoid misinformation in case reduce already start reading that file from the existing map worker: master can just reject this update

and for reduce task this is to not corrupt the output file that is being written to the shared storage: writing to a tmp file and then os rename can handle this
