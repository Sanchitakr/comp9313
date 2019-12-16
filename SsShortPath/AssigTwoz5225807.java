/* @Author Sanchita K Raghuprasad 
A path in a graph can be defined as the set of consecutive nodes such that there is an edge from one node to the next node in the sequence. 
The shortest path between two nodes can be defined as the path that has the minimum total weight of the edges along the path.
This assignment is divided into 4 steps to get the final formatted outcome.

* STEP 1 : Graph
 The task is to read the input argument which will then be be used to generate adjacency list and stores it in a custom class called "Graph_Details".
  Detail is associated with node_id , distance_to_source and adjacency_list.
  Graph_Details class stores all necessary info for computing 
  the shortest path.
  
* STEP 2 : Emit
  
  Algorithm used: Parallel BFS. 
  1.For each node, the program iterates through adjacency list (key:adjacent_node , value:weight). 
  2.For each adjacent node in the list, it creates a (node_id, details) pair.
	   where,
	    node_id is the id of the adjacent node 
		details is the Graph_Details class of the adjacent node. 
  3.The shortest distance to the source of the adjacent node = shortest distance of current node +  weight(edge) 
	if the shortest distance of current node is unknown, then the shortest distance of the adjacent node is infinity represented as -1
  
* STEP 3 : Compare and Reduce
  
  For some nodes , there might be multiple (node_id, details) pairs This infers that there are multiple distinct paths from source to destination node 
  So we compare them and find the shortest one. The result of each iteration is also stored in the temporary directory. 
  It(Step 2 and Step 3 ) keeps iterating until there is no update on shortest paths in that iteration.
  
* STEP 4 :  output
  
  The output from Step 3 are (node_id, details) pairs, it then extracts shortest distance and path information from the Graph_Details class
  and sort (sorted by keys) them in ascending order based on shortest distance and put it on the output text file 
  
Source:
https://github.com/yilmazburk/Parallel-BFS/blob/master/src/com/parallel/bfs/Graph.java
https://medium.com/@KerrySheldon/breadth-first-search-in-apache-spark-d274403494ca
https://www.infoq.com/articles/apache-spark-graphx/
*/
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class AssigTwoz5225807 {

	// stores complete details of a node
	private static class Graph_Details implements Serializable{
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		String node_id;  
		int distance_to_source;
		HashMap<String, Integer> adjacency_list; 
		String path; 
		
		public Graph_Details(String node_id,String source_node,String adjacent_node, int weight) {
			this.node_id = node_id;
			// if its source node, distance to source to 0, else infinity
			this.distance_to_source = (node_id.equals(source_node))?0:Integer.MAX_VALUE; 
			this.adjacency_list = new HashMap<String,Integer>();
			if (node_id.equals(source_node)) {
				this.path = node_id;
			} else {
				this.path = "-" + node_id;
			}
			adjacency_list.put(adjacent_node, weight);
			
		}
		
		public Graph_Details(String node_id) {
			this.node_id = node_id;
			this.distance_to_source = Integer.MAX_VALUE;
			this.adjacency_list = new HashMap<String,Integer>();
			this.path = "-" + node_id;
		}
		
		
		public void merge_adjacent_list (Graph_Details target_node) {
			
			this.adjacency_list.putAll(target_node.getAdjacency_list());
		}

		public HashMap<String, Integer> getAdjacency_list() {
			return adjacency_list;
		}


		public int getDistance_to_source() {
			return distance_to_source;
		}


		public void setDistance_to_source(int new_distance) {
			if (this.distance_to_source >= new_distance) {
				this.distance_to_source = new_distance;
			}
		}
		
		
		public String getPath() {
			return path;
		}
		
		public void setPath(String path) {
			this.path = path;
		}

        
		// Iterates through adjacency list, create (node_id, details) pairs 
		//stores shortest distance and paths in the graph details object 
		public ArrayList<Tuple2<String,Graph_Details>> emit(){
			ArrayList<Tuple2<String,Graph_Details>> emit = new ArrayList<Tuple2<String,Graph_Details>>();
			for (Map.Entry<String, Integer> node : adjacency_list.entrySet()) {
				String adjacent_node_id = node.getKey();
				int edge_weight = node.getValue();
				Graph_Details adjacent_node_detail = new Graph_Details(adjacent_node_id);
				int new_ds = 0;
				
				
				if (getDistance_to_source() == Integer.MAX_VALUE) {
					new_ds = Integer.MAX_VALUE;
					adjacent_node_detail.setPath("");
				
				} else {
					new_ds = edge_weight + getDistance_to_source();
					String updated_path = this.path + adjacent_node_detail.getPath();
					adjacent_node_detail.setPath(updated_path);
					
				}
				adjacent_node_detail.setDistance_to_source(new_ds);
				emit.add(new Tuple2<String,Graph_Details>(adjacent_node_id,adjacent_node_detail));
			}
			
			emit.add(new Tuple2<String,Graph_Details>(node_id,this));
			
			return emit;
			
		}
		//if the shortest distance of current node is unknown, then the shortest distance of the adjacent node is infinity represented as -1
		@Override
		public String toString() {
			if (distance_to_source == Integer.MAX_VALUE) {
				distance_to_source = -1;
				path = "";
			}
			return node_id + "," + distance_to_source + "," + this.path;
			
		}
		
		
		
	}
	
	
	public static void main(String [] args) throws IOException {
		
		
		
		SparkConf conf = new SparkConf()
				.setMaster("local")
				.set("spark.hadoop.validateOutputSpecs", "false")
				.setAppName("Assignment2");
		
		JavaSparkContext context = new JavaSparkContext(conf);
		
		
		/*=====================================================
		 
		STEP 1 : Graph 

		=======================================================*/
		JavaRDD<String> input = context.textFile(args[1]);
		
		String source_node = args[0];
		
		//create a (node_id, details) pair, the adjacency_list and stores it in the graph details object
		JavaPairRDD<String,Graph_Details> node_details = input.mapToPair(new PairFunction<String,String,Graph_Details>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String,Graph_Details> call(String line) throws Exception {
				
				String[] parts = line.split(",");
				String start_node = parts[0];
				String end_node = parts[1];
				int weight = Integer.parseInt(parts[2]);
				
				Graph_Details details = new Graph_Details(start_node,source_node,end_node,weight);
				
				return new Tuple2<String,Graph_Details>(start_node,details);
				
			}
				
		});
		
		// reduce the (node_id, details) pairs, by merging the adjacency list of same nodes
		JavaPairRDD<String,Graph_Details> graph = node_details.reduceByKey((Function2<Graph_Details,Graph_Details,Graph_Details>) (d1,d2) -> {
			d1.merge_adjacent_list(d2);
			return d1;
		});
		

		
		while (true) {	
		/*=====================================================
		 
		STEP 2 : Emit 

		=======================================================*/
			//  current status 
			JavaPairRDD<String,Integer> old_shortest_paths = get_shortest_paths(graph);

			// for each (node_id, details) pairs, iterate the adjacency list and generate multiple (node_id, details) pairs
			graph = graph.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Graph_Details>,String,Graph_Details>(){
		
				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				@Override
				public Iterator<Tuple2<String, Graph_Details>> call(Tuple2<String, Graph_Details> node_tuple) throws Exception {
					Graph_Details node_detail = node_tuple._2;
					return node_detail.emit().iterator();
				}
				
			});
		/*=====================================================
		 
		STEP 3 : Compare and reduce 

		=======================================================*/
			//reduce multiple (node_id,detail) pairs by comparing them and find the shortest one
			graph = graph.reduceByKey((Function2<Graph_Details,Graph_Details,Graph_Details>) (d1,d2) -> {
				d1.merge_adjacent_list(d2);
				
				// compare the distance to sources and choose the smaller one as the shortest distance
				if (d1.getDistance_to_source() > d2.getDistance_to_source()) {
					d1.setDistance_to_source(d2.getDistance_to_source());
					d1.setPath(d2.getPath());
				} else if (d1.getDistance_to_source() < d2.getDistance_to_source()){
					d1.setDistance_to_source(d1.getDistance_to_source());
				} 
				
				return d1;
			});
			
			//  current status
			JavaPairRDD<String,Integer> new_shortest_paths = get_shortest_paths(graph);
			
			// if there is no updates, break
			if (!if_updated(old_shortest_paths, new_shortest_paths)) {
				break;
			}
		
		}
		/*=====================================================
		 
		STEP 4 : Output 

		=======================================================*/
		// remove source node from output
		graph = graph.filter(new Function<Tuple2<String,Graph_Details>,Boolean>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Graph_Details> node_info) throws Exception {
				
				return (node_info._1.equals(args[0]))?false:true;
			}
			
		});
		
		
		// The output sorted (ascending) by length of shortest path.
		JavaPairRDD<Integer,Graph_Details> sorted_by_shortest_distances = graph.mapToPair(new PairFunction<Tuple2<String,Graph_Details>,Integer,Graph_Details>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, Graph_Details> call(Tuple2<String, Graph_Details> tuple) throws Exception {
				int sp = tuple._2.getDistance_to_source();
			    Graph_Details detail = tuple._2();	
				return new Tuple2<Integer,Graph_Details>(sp,detail);
			}
			
		}).sortByKey();
		
		// output converted javaRDD string to eliminate parenthesis
		JavaRDD<String> result = sorted_by_shortest_distances.map(new Function<Tuple2<Integer,Graph_Details>,String>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<Integer, Graph_Details> tuple) throws Exception {
				
				return tuple._2.toString();
			}
			
		});
		result.saveAsTextFile(args[2]);
		
	}
	
	
	/* 1. get_shortest_paths: given (node_id, details) pairs, return (node_id, sp) pairs, where sp is the shortest distance (an integer)
	 		Used for detecting if there is any updates of shortest distances.
	   2. if_updated: given two collections of (node_id, sp) pairs returned by get_shortest_paths, return a boolean that says if there is any updates
			 in the shortest distances
	 */
	
	public static JavaPairRDD<String,Integer> get_shortest_paths(JavaPairRDD<String,Graph_Details> graph){
		JavaPairRDD<String,Integer> shortest_paths = graph.mapToPair(new PairFunction<Tuple2<String,Graph_Details>,String,Integer>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Graph_Details> node_info) throws Exception {
				String node_id = node_info._1;
				Integer shortest_distance = node_info._2.getDistance_to_source();
				
				return new Tuple2<String,Integer>(node_id,shortest_distance);
			}
			
		});
		
		return shortest_paths;
		
	}
	
	 
	public static boolean if_updated(JavaPairRDD<String,Integer> g1,JavaPairRDD<String,Integer> g2) throws IOException {
		
		JavaPairRDD<String,Tuple2<Integer,Optional<Integer>>> temp = g1.leftOuterJoin(g2);
		
		
		
		JavaPairRDD<String,Integer> merged = temp.mapToPair(new PairFunction<Tuple2<String,Tuple2<Integer,Optional<Integer>>>,String,Integer>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Tuple2<Integer, Optional<Integer>>> old_and_new_shortest_path)
					throws Exception {
				
				int old_sp = old_and_new_shortest_path._2._1;
				int new_sp = old_and_new_shortest_path._2._2.get();
				//if the old_shortest_distance equals to the new_shortest_distance then if_updated is 0 , otherwise 1.
				int if_updated = (old_sp == new_sp)?0:1;
				
				return new Tuple2<String,Integer>("number of updates",if_updated);
			}
			
		});
		
		
		
		
		merged = merged.reduceByKey((Function2<Integer,Integer,Integer>) (n1,n2) -> {
			return n1 + n2;// where the num_of_updates is the total number of updates in the graph
		});
		
		
		JavaRDD<Integer> result = merged.map(new Function<Tuple2<String,Integer>,Integer>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Tuple2<String, Integer> num_updates_tuple) throws Exception {
				int num_updates = num_updates_tuple._2;
				return num_updates;
			}
			
		});
		//It then stores the num_of_updates in a folder called "temporary", then parse through it. 
		result.saveAsTextFile("temporary");
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("temporary/part-00000")));
		
		int num_updates = Integer.parseInt(reader.readLine());
		
	
		return (num_updates != 0)?true:false;

	}
}

