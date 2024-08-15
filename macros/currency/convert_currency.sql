--- convert(total_arr, 'EUR')
{% macro convert_currency(amount_column, currencyisocode, target_currency, currency_date) -%}


    {% if target_currency == 'EUR'  -%}

    CASE WHEN {{ currencyisocode }} = 'USD' then 
      CASE WHEN DATE({{ currency_date }}) < "2022-03-01" THEN {{ amount_column }} / 1.18465 
      WHEN DATE({{ currency_date }}) >= "2022-03-01" THEN {{ amount_column }} / 1.01 END 
    ELSE {{ amount_column }} 
    END
        
    
    {% endif -%}

    
  {% if target_currency == 'USD'  -%}

    CASE WHEN {{ currencyisocode }} = 'EUR' THEN 
      CASE WHEN DATE({{ currency_date }}) < "2022-03-01" THEN {{ amount_column }} * 1.18465 
      WHEN DATE({{ currency_date }}) >= "2022-03-01" THEN {{ amount_column }} * 1.01 END
     
    ELSE {{ amount_column }} 
    END     
    {% endif -%}

{% endmacro %}