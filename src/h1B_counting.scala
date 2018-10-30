import scala.io.Source
import scala.collection.mutable.{Map,HashMap,PriorityQueue}
import java.io._

object H1BStats {
  def make_filter_on(filter_on: List[Tuple2[String,String]], headers: Array[String]): List[Tuple2[Int,String]]={
    var return_value=filter_on.map(filter_on_variable=>(headers.indexOf(filter_on_variable._1),filter_on_variable._2))
    return_value=return_value.filterNot(x=>(x._1==(-1)))
    return_value
  }
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
  def meets_criteria(current_line: Array[String], criteria: List[Tuple2[Int,String]]) : Boolean ={
    criteria.forall(z=>meets_criterion(current_line,z))
  }
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
  def cleanString(unformated_string: String) : String={
    unformated_string.replace("\"","")
  }
  def make_heap(this_dict : Map[String,Int]) : PriorityQueue[Tuple2[Int,String]] = {
    val ord:Ordering[(Int,String)] = Ordering.Tuple2(Ordering.Int,Ordering.String.reverse)
    val pq=new PriorityQueue[Tuple2[Int,String]]()(ord)
    val seq_to_add=this_dict.toIndexedSeq.map(z=>(z._2,cleanString(z._1)))
    pq++seq_to_add
  }
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