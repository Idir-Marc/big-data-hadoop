import java.io.IOException;
import java.util.Arrays;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class LOWER extends EvalFunc<String> {
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try {
            String str = (String) input.get(0);
            return (str.toLowerCase());
        } catch (Exception e) {
            throw new IOException("Caught exception processing input row ", e);
        }
    }
}
