package kpipes.binding.view

interface KeyValueMaterializedView<T> {

    void put(String tenant, String namespace, String key, T event)

    void remove(String tenant, String namespace, String key)

    Map<String, T> list(String tenant, String namespace)

}
