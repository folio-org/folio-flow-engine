package org.folio.flow.api;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.folio.flow.impl.StageContextImpl;

public interface StageContext {

  /**
   * Returns a current flow identifier.
   *
   * @return current flow identifier.
   */
  String flowId();

  /**
   * Retrieves flow parameters as {@link Map} object.
   *
   * <p>Flow parameters are merged with upstream flow, current flow parameters are prioritized</p>
   *
   * @return {@link Map} as representation of stage context parameters.
   */
  Map<Object, Object> flowParameters();

  /**
   * Retrieves flow parameters by {@link Object} key as {@link R} value.
   *
   * @param <R> - generic type for flow parameter output
   * @return flow parameter by {@link Object} key.
   * @throws ClassCastException if the value is not of the expected type
   */
  <R> R getFlowParameter(Object key);

  /**
   * Puts some arbitrary data in the context.
   *
   * @param key - key as {@link Object} to put value to the context data holder
   * @param obj - value as {@link Object} to put to the context data holder
   * @return a reference to this, so the API can be used fluently
   */
  StageContext put(Object key, Object obj);

  /**
   * Retrieves a value by key from the context.
   *
   * @param key -  key as {@link Object} to retrieve value from the context data holder
   * @param <T> - generic type for returned value
   * @return value by key from context data holder as {@link T} object
   * @throws ClassCastException if the value is not of the expected type
   */
  <T> T get(Object key);

  /**
   * Retrieves a value by key from the context.
   *
   * @param key -  key as {@link Object} to retrieve value from the context data holder
   * @param valueClass - return class identifier
   * @param <T> - generic type for returned value
   * @return value by key from context data holder as {@link T} object
   * @throws ClassCastException if the value is not of the expected type
   */
  <T> T get(Object key, Class<T> valueClass);

  /**
   * Retrieves a value by key from the context.
   *
   * @param key -  key as {@link Object} to retrieve value from the context data holder
   * @param defaultValue - default value if corresponding value is not found by key in the context data holder.
   * @param <T> - generic type for returned value
   * @return value by key from context data holder as {@link T} object
   * @throws ClassCastException if the value is not of the expected type
   */
  <T> T get(Object key, T defaultValue);

  /**
   * Returns context data as {@link Map} object.
   *
   * @return context data as {@link Map}
   */
  Map<Object, Object> data();

  /**
   * Creates a default implementation {@link StageContext} from execution context, flow parameters and stage data.
   *
   * @param flowId - flow identifier as {@link String} value
   * @param flowParameters - flow parameters as {@link Map} object
   * @param data - mutable {@link Map} for stage-to-stage parameters
   * @return created {@link StageContext} object
   */
  static StageContext of(Object flowId, Map<?, ?> flowParameters, Map<?, ?> data) {
    return new StageContextImpl(flowId, flowParameters, data);
  }

  /**
   * Creates a copy for stage context object.
   *
   * @return a created copy for {@link StageContext} object
   */
  static StageContext copy(StageContext context) {
    return new StageContextImpl(context.flowId(), context.flowParameters(), new HashMap<>(context.data()));
  }

  /**
   * Creates a copy for stage context object.
   *
   * @return a created copy for {@link StageContext} object
   */
  default StageContext withFlowId(String flowId) {
    if (Objects.equals(flowId, this.flowId())) {
      return this;
    }

    return StageContext.of(flowId, flowParameters(), data());
  }
}
