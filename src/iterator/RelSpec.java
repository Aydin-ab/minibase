package iterator;

public class RelSpec {
  public int key;
  
  /**
   *constructor
   *@param value the enum value
   */
  public  RelSpec (int value) {
    key = value;
  }
  
  public final static int outer = 0;
  public final static int innerRel = 1;
}  

