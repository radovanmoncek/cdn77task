package anonymizer;

/**
  * This interface follows the DAO design pattern.
  * @typeparam T the type of Object being stored (useful for ORM)
  * https://www.geeksforgeeks.org/system-design/data-access-object-pattern/
  */
public interface DAO<T> {
	/**
	  * Save an Object of type T to some encapsulated database.
	  */
	T save(T object);
	/**
	  * Query the database for an Object T of id.
	  * @param id of the desired T object
	  */
	T query(int id) throws Exception;
	/**
	  * Query the database for any object, return the result immediately.
	  */
	T queryNoWait() throws Exception;
}
