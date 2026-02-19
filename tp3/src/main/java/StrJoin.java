import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.util.List;
import java.util.stream.Collectors;

public class StrJoin extends EvalFunc<String> {
    @Override
    public String exec(Tuple input) {
        if (input == null || input.size() == 0) {
            return null;
        }
        List<String> list = input.getAll().stream()
                .map(Object::toString)
                .collect(Collectors.toList());
        return String.join(",", list);
    }
}
