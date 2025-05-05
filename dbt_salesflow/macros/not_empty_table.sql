{% test not_empty_table(model) %}
    select count(*) as row_count
    from {{ model }}
    having row_count = 0
{% endtest %}
