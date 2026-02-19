import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

public class ANAGRAM extends EvalFunc<String> {
    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        Object o = input.get(0);
        if (!(o instanceof String)) {
            throw new IOException("Expected a string, got a "+o.getClass());
        }
        String s = (String) o;
        return s.chars().sorted()
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }
}
