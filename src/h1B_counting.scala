import scala.io.Source
import scala.collection.mutable.{Map,HashMap,PriorityQueue}
import java.io._

object H1BStats {
  
  /*
  Suppose filter_on is [(CASE_STATUS,CERTIFIED),(EMPLOYER_STATE,CA)] which give the criterion for whether a worker is counted
  or not and headers is the array of column titles in input.csv. One of those indices is for CASE_STATUS (say i) and one is EMPLOYER_STATE (j)
  This function then returns [(i,CERTIFIED),(j,CA)]. If we have some other criterion like (JUNK,DONT_CARE), then
  JUNK is not one of the headers so it isn't found. In that case instead of having an entry (-1,DONT_CARE), that criterion is removed.
  */
  def make_filter_on(filter_on: List[Tuple2[String,String]], headers: Array[String]): List[Tuple2[Int,String]]={
    var return_value=filter_on.map(filter_on_variable=>(headers.indexOf(filter_on_variable._1),filter_on_variable._2))
    return_value=return_value.filterNot(x=>(x._1==(-1)))
    return_value
  }
  /*
  The previous function has made a list of criterion. For a single one of them, this checks if the line of input
  representing a single worker as current_line satisfies that criterion.
  That is if the criterion says the i'th entry of this row should say CERTIFIED, then this checks that.
  In case, the current_line does not have an i'th entry, it is treated as false. This should only happen
  if there is a mismatch in number of columns as compared with the header line of input.csv
  */
  def meets_criterion(current_line: Array[String], criterion: Tuple2[Int,String]) : Boolean ={
    var return_value=false
    try{
      return_value=(current_line(criterion._1)==criterion._2)
    }
    catch{
      case t:Throwable => return_value=false
    }
    return_value
  }
  /*
  Combine the information from all the critiria to see if the worker does or does not meet all of them.
  */
  def meets_criteria(current_line: Array[String], criteria: List[Tuple2[Int,String]]) : Boolean ={
    criteria.forall(z=>meets_criterion(current_line,z))
  }
  /*
  entries is the current_line for a given worker
  current_dict is (i,dictionary) where i represents the column index of one of the K different things we are counting.
  the dictionary is the one keeping track of counts for that variable.
  we look at entries[i] to see something like this worker is a ENGINEER and increment the count of ENGINEER by 1
  */
  def modify_for_this_line(entries: Array[String],current_dict: Tuple2[Int,Map[String,Int]]): Unit={
    try{
      val current_var_for_this_line=cleanString(entries(current_dict._1))
      val current_count=current_dict._2.getOrElseUpdate(current_var_for_this_line,0)
      current_dict._2(current_var_for_this_line)=(current_count+1)
    }
    catch{
      case t:Throwable => {
        throw new IllegalArgumentException("Mismatch in number of columns compared to the header line")
      }
    }
  }
  /*
  Open the file and read line by line. Check if this line is valid using the meets_criteria function.
  Then for each of the K variables being counted use the modify_for_this_line function on the given line.
  Repeat for all the workers representing individual lines in the input_file
  */
  def make_dicts(input_file: String, filter_on: List[Tuple2[String,String]], variables_to_count: List[String]) : (List[Map[String,Int]],Int) = {
    val bufferedSource=Source.fromFile(input_file)
    val headers = bufferedSource.getLines.next().split(";")
    val filter_on_refined=make_filter_on(filter_on,headers)
    val variables_to_count_indices=variables_to_count.map(current_var=>headers.indexOf(current_var))
    if (variables_to_count_indices.contains(-1)){
      throw new IllegalArgumentException("Asking to count something that isn't in the header")
    }
    val indices_and_all_the_dicts=variables_to_count_indices.map(z=>(z,new HashMap[String,Int]))
    var total_to_count=0
    for (line <- bufferedSource.getLines) {
      val entries=line.split(";").toArray
      if (meets_criteria(entries,filter_on_refined)){
        indices_and_all_the_dicts.foreach(z=>modify_for_this_line(entries,z))
        total_to_count=total_to_count+1
      }
    }
    bufferedSource.close
    (indices_and_all_the_dicts.map(z=>z._2),total_to_count)
  }
  /*Remove extraneous quotation marks*/
  def cleanString(unformated_string: String) : String={
    unformated_string.replace("\"","")
  }
  /*
  Take each dictionary that says for example (ENGINEER,5),(PROFESSOR,1),(ADMINISTRATOR,5) and put them into a heap
  so that the 5 administrators are at the top because they are tied with engineers with the most, but beat them in
  alphabetical order.
  */
  def make_heap(this_dict : Map[String,Int]) : PriorityQueue[Tuple2[Int,String]] = {
    val ord:Ordering[(Int,String)] = Ordering.Tuple2(Ordering.Int,Ordering.String.reverse)
    val pq=new PriorityQueue[Tuple2[Int,String]]()(ord)
    val seq_to_add=this_dict.toIndexedSeq.map(z=>(z._2,cleanString(z._1)))
    pq++seq_to_add
  }
  /*
  give this_output as combining the information of the header for the first column representing what was being counted.
  along with the location of the file where we are supposed to write along with the PriorityQueue constructed by the make_heap
  also provide how many workers were counted by meeting all the criteria and the top_how_many that should be written to the file.
  Then one by one dequeue from the PriorityQueue and write the line with the associated string, the count and count/total_counted as
  a percentage.
  When the queue is empty and throws an error when being dequeue'd we stop.
  */
  def write_output(this_output: Tuple2[Tuple2[String,String],PriorityQueue[Tuple2[Int,String]]], total_counted: Int, top_how_many : Int) : Unit={
    val ((output_file,header),pq)=this_output
    val file=new File(output_file)
    val bw = new BufferedWriter(new FileWriter(file,false))
    bw.write(header+";NUMBER_CERTIFIED_APPLICATIONS;PERCENTAGE\n")
    var i = 1
    var should_continue=true
    while (should_continue &&  i<=top_how_many){
      try{
        val (count,label)=pq.dequeue()
        val percentage=count/(total_counted+0.0)*100
        bw.write(label+";"+count+";"+"%2.1f".format(percentage)+"%\n")
      }
      catch{case t:Throwable => {should_continue=false}}
      i=i+1
    }
    bw.close()
  }
  
  /*
  Build the dictionaries that count the K variables with the make_dicts function.
  Turn each of them into heaps with make_heap
  Then use write_output on each of the K output files
  */
  def main(args: Array[String]): Unit = {
    //val criteria=List(("CASE_STATUS","CERTIFIED"),("UNUSED","DONT CARE"))
    val criteria=List(("CASE_STATUS","CERTIFIED"))
    //val variables_to_count=List("EMPLOYER_STATE","SOC_NAME","WORKSITE_STATE")
    val variables_to_count=List("SOC_NAME","WORKSITE_STATE")
    val top_how_many=10
    val output_files=args.tail.toList
    if (output_files.length != variables_to_count.length){
      throw new IllegalArgumentException("Not the right number of output files specified")
    }
    val (counted_dicts,total_counted)=make_dicts(args(0),criteria,variables_to_count)
    val counted_heaps=counted_dicts.map(make_heap)
    val output_files_headers=List("TOP_OCCUPATIONS","TOP_STATES")
    val output_results=output_files.zip(output_files_headers)
    val output_results_two=output_results.zip(counted_heaps)
    output_results_two.foreach(this_output=>write_output(this_output,total_counted,top_how_many))
  }
}