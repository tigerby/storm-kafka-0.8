package storm.kafka;

/**
 * Created by tigerby on 2/26/14.
 *
 * @author <a href="mailto:bongyeonkim@gmail.com">Kim Bongyeon</a>
 * @version 1.0
 */
public interface FetchHandler {

    void updateMetrics(long latency);
}
