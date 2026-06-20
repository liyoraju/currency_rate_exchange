{% macro generate_schema_name(custom_dataset_name, node) -%}

    {%- set default_dataset = target.dataset -%}
    {%- if custom_schema_name is none -%}

        {{ default_dataset }}

    {%- else -%}

        {{ custom_dataset_name | trim }}

    {%- endif -%}

{%- endmacro %}