package org.sample.batch.csv;

import com.google.common.base.Strings;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * CsvNameExtractor helps to handle CSV bean fields mapping.
 * <p>
 * Warn: does not handle inheritance.
 * </p>
 *
 * @param <T> Bean Type with fields annotated with {@link Column}
 */
public class CsvNameExtractor<T> {

  private final Class<T> type;

  public CsvNameExtractor(Class<T> type) {
    this.type = type;
//        getFieldStream(type, false).forEach(System.out::println);
  }

  private static Stream<Field> getFieldStream(Class<?> type, boolean includeOptional) {
    return Arrays.stream(type.getDeclaredFields())
      .filter(e -> e.getAnnotation(Column.class) != null && (includeOptional || !e.getAnnotation(Column.class).optional()))
      .sorted((f1, f2) -> {
        Column c1 = f1.getAnnotation(Column.class);
        Column c2 = f2.getAnnotation(Column.class);
        return c1.position() - c2.position();
      });
  }

  /**
   * Returns the columns names of fields annotated with {@link Column}.
   *
   * @return
   */
  public List<String> getColumnNames() {
    return getColumnNames(true);
  }

  public List<String> getColumnNames(boolean includeOptional) {
    return getFieldStream(this.type, includeOptional)
      .map(field -> {
        final Column column = field.getAnnotation(Column.class);
        return Strings.isNullOrEmpty(column.value()) ? field.getName() : column.value();
      })
      .collect(Collectors.toList())
      ;
  }

  /**
   * Returns the names of fields annotated with {@link Column}.
   *
   * @return
   */
  public List<String> getNames() {
    return getNames(true);
  }

  public List<String> getNames(boolean includeOptional) {
    return getFieldStream(this.type, includeOptional)
      .map(field -> field.getName())
      .collect(Collectors.toList())
      ;
  }

  /**
   * Returns the list of values annotated with {@link Column}.
   *
   * @param item
   * @return
   */
  public List<String> getValues(T item) {
    return getValues(item, true);
  }

  public List<String> getValues(T item, boolean includeOptional) {
    return getFieldStream(this.type, includeOptional)
      .map(field -> {
        field.setAccessible(true);
        Object value = null;
        try {
          value = field.get(item);
        } catch (IllegalAccessException e) {
          throw new IllegalStateException(e);
        }
        return value == null ? "" : value.toString();
      })
      .collect(Collectors.toList())
      ;
  }

  public T beanFrom(String[] columnValues) throws IllegalAccessException, InstantiationException {
    T bean = this.type.newInstance();
    List<Field> fields = getFieldStream(this.type, true).collect(Collectors.toList());
    IntStream
      .range(0, Math.min(columnValues.length, fields.size()))
      .forEach(idx -> {
        Field field = fields.get(idx);
        field.setAccessible(true);
        try {
          field.set(bean, columnValues[idx]);
        } catch (Exception ignore) {
//                    throw new RuntimeException(e);
        }
      });
    return bean;
  }

  public Map<String, String> from(String[] columnValues, boolean includeOptional) {
    List<Field> fields = getFieldStream(this.type, includeOptional).collect(Collectors.toList());
    return IntStream
      .range(0, Math.min(columnValues.length, fields.size()))
      .boxed()
      .collect(Collectors.toMap(idx -> fields.get(idx).getName(), idx -> columnValues[idx],
        (oldValue, newValue) -> oldValue, LinkedHashMap::new));
  }

  public Map<String, String> nonAvailable(boolean includeOptional) {
    return getFieldStream(this.type, includeOptional)
      .collect(Collectors.toMap(
        field -> field.getName(),
        field -> "N/A"
        , (oldValue, newValue) -> oldValue, LinkedHashMap::new));
  }

  public Map<String, String> from(T item, boolean includeOptional) {
    return getFieldStream(this.type, includeOptional)
      .collect(Collectors.toMap(
        field -> field.getName(),
        field -> {
          field.setAccessible(true);
          Object value = null;
          try {
            value = field.get(item);
          } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
          }
          return value == null ? "" : value.toString();
        }
        , (oldValue, newValue) -> oldValue, LinkedHashMap::new));
  }

}