{% macro add_pk_constraint(model_name, column_name) %}
    alter table {{ model_name }} modify column {{ column_name }} set not null;
    alter table {{ model_name }} add constraint pk_{{ model_name.name }} primary key ({{ column_name }})
{% endmacro %}

{% macro add_fk_constraint(model_name, column_name, reference_model, reference_column) %}
    alter table {{ model_name }} add constraint fk_{{ model_name.name }}_{{ reference_model.name }} foreign key ({{ column_name }}) references {{ reference_model }} ({{ reference_column }})
{% endmacro %}
