package ed.inf.adbs.blazedb;

import java.util.*;

/**
 * The Tuple class represents a row of data.
 *
 * You will need to modify this class, obviously :).
 */

public class Tuple {
    private List<String> values;

    public Tuple(List<String> values) {
        this.values = new ArrayList<>(values);
    }

    public String getValue(int index) {
        return values.get(index);
    }

    public List<String> getValues() {
        return Collections.unmodifiableList(values);
    }

    @Override
    public String toString() {
        return String.join(",", values);
    }
}
