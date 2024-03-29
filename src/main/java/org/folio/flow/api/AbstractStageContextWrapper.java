package org.folio.flow.api;

import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString
@EqualsAndHashCode
public abstract class AbstractStageContextWrapper implements StageContext {

  protected final StageContext context;

  protected AbstractStageContextWrapper(StageContext stageContext) {
    this.context = StageContext.copy(stageContext);
  }

  @Override
  public String flowId() {
    return context.flowId();
  }

  @Override
  public Map<Object, Object> flowParameters() {
    return context.flowParameters();
  }

  @Override
  public <R> R getFlowParameter(Object key) {
    return context.getFlowParameter(key);
  }

  @Override
  public StageContext put(Object key, Object obj) {
    return context.put(key, obj);
  }

  @Override
  public StageContext remove(Object key) {
    return context.remove(key);
  }

  @Override
  public <T> T get(Object key) {
    return context.get(key);
  }

  @Override
  public <T> T get(Object key, Class<T> valueClass) {
    return context.get(key, valueClass);
  }

  @Override
  public <T> T get(Object key, T defaultValue) {
    return context.get(key, defaultValue);
  }

  @Override
  public Map<Object, Object> data() {
    return context.data();
  }

  @Override
  public StageContext withFlowId(String flowId) {
    return context.withFlowId(flowId);
  }
}
