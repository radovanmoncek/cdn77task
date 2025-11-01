package anonymizer;

public interface DAO<T> {
	T save(T object);
	T query(int id) throws Exception;
	T queryNoWait() throws Exception;
}
