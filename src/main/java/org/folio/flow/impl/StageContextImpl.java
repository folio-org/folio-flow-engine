package org.folio.flow.impl;

import static java.util.Collections.unmodifiableMap;
import static org.folio.flow.utils.FlowUtils.FLOW_ENGINE_LOGGER_NAME;
import static org.folio.flow.utils.FlowUtils.requireNonNull;

import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.folio.flow.api.StageContext;

/**
 * Stage context implementation with immutable flow parameters and mutable stage data.
 */
@Data
@Log4j2(topic = FLOW_ENGINE_LOGGER_NAME)
public final class StageContextImpl implements StageContext {

  /**
   * Flow identifier.
   */
  private final String flowId;

  /**
   * Global flow parameters.
   */
  private final Map<Object, Object> flowParameters;

  /**
   * Stage-defined parameters.
   */
  private final Map<Object, Object> data;

  /**
   * A default constructor.
   *
   * @param flowId - flow identifier as {@link String} value
   * @param flowParameters - flow parameters, non-null
   * @param data - stage data parameters, non-null
   */
  public StageContextImpl(Object flowId, Map<?, ?> flowParameters, Map<?, ?> data) {
    requireNonNull(data, "Stage parameters map must not be null");
    requireNonNull(flowParameters, "Flow parameters map must not be null");

    this.flowId = requireNonNull(flowId, "Flow identifier must not be null").toString();
    this.flowParameters = unmodifiableMap(flowParameters);
    //noinspection unchecked
    this.data = data instanceof HashMap<?, ?> ? (Map<Object, Object>) data : new HashMap<>(data);
  }

  @Override
  public String flowId() {
    return this.flowId;
  }

  @Override
  public <R> R getFlowParameter(Object key) {
    //noinspection unchecked
    return (R) this.flowParameters.get(key);
  }

  @Override
  public StageContext put(Object key, Object obj) {
    data.put(key, obj);
    return this;
  }

  @Override
  public StageContext remove(Object key) {
    data.remove(key);
    return this;
  }

  @Override
  public <T> T get(Object key) {
    //noinspection unchecked
    return (T) data.get(key);
  }

  @Override
  public <T> T get(Object key, Class<T> valueClass) {
    //noinspection unchecked
    return (T) data.get(key);
  }

  @Override
  public <T> T get(Object key, T defaultValue) {
    //noinspection unchecked
    return (T) data.getOrDefault(key, defaultValue);
  }

  @Override
  public Map<Object, Object> flowParameters() {
    return this.flowParameters;
  }

  @Override
  public Map<Object, Object> data() {
    return this.data;
  }
}
