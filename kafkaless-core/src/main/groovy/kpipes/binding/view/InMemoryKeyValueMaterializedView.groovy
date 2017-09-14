package kpipes.binding.view

class InMemoryKeyValueMaterializedView<T> implements KeyValueMaterializedView<T> {

    private final Map<String,Map<String,Map<String, T>>> events = [:].withDefault {[:].withDefault {[:]}}

    void put(String tenant, String namespace, String key, T event) {
        events[tenant][namespace][key] = event
    }

    @Override
    void remove(String tenant, String namespace, String key) {
        events[tenant][namespace].remove(key)
    }

    Map<String, T> list(String tenant, String namespace) {
        events[tenant][namespace]
    }

}
