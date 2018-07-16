package Spark.SparkBasicOperations;

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;

/**
 * A factory class used to construct {@link Row} objects.
 *
 * @since 1.3.0
 */
@InterfaceStability.Stable
public class myRowFactory {

  /**
   * Create a {@link Row} from the given arguments. Position i in the argument list becomes
   * position i in the created {@link Row} object.
   *
   * @since 1.3.0
   */
  public static Row create(Object ... values) {
	  String[] fields=null;
	  for (Object value: values){
		  System.out.println("values : "+value);  
		  fields = ((String) value).split(",");
	  }
	  
	  return new GenericRow(fields);
  }
}
