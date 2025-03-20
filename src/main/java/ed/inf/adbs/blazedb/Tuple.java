package ed.inf.adbs.blazedb;

import java.util.*;

/**
 * The Tuple class represents a row of data.
 *
 * You will need to modify this class, obviously :).
 */

public class Tuple {
    private List<Integer> values;

    public Tuple(List<Integer> values) {
        this.values = new ArrayList<>(values);
    }

    public Tuple() {}

    public Integer getValue(int index) {
        return values.get(index);
    }

    public List<Integer> getValues() {
        return Collections.unmodifiableList(values);
    }

    @Override
    public String toString() {
        return String.join(", ", values.stream().map(String::valueOf).toArray(String[]::new));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tuple tuple = (Tuple) o;
        return Objects.equals(values, tuple.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(values);
    }
}
