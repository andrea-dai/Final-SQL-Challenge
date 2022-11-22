USE magist;

/****
CHALLENGE 1 Expand the database
*****/

# download the 'list-states-brazil-38j.csv' from online and insert this table to Schemas' magist'
SELECT * FROM states_brazil;

/**** 
CHALLENGE 2 Analyze customer review
****/

/****
2.1 Find the average review score by state of the customer
****/

SELECT 
	#customer_id,
    DISTINCT sb.State,
    AVG (review_score) OVER(PARTITION BY (sb.State)) AS avg_review_state
FROM 
    order_reviews
        LEFT JOIN 
	orders using (order_id)
        LEFT JOIN 
    customers AS cu using(customer_id)
        LEFT JOIN 
    geo AS ge ON cu.customer_zip_code_prefix = ge.zip_code_prefix
        LEFT JOIN 
	states_brazil AS sb ON ge.state = sb.Abbreviation;

/******
2.2 Do reviews containing positive words have a better score? Some Portuguese positive words are: 
“bom”, “otimo”, “gostei”, “recomendo” and “excelente”.
*******/

DROP TABLE IF EXISTS positive_review;

CREATE TEMPORARY TABLE positive_review
SELECT 
    review_comment_message,
    review_id,
    review_score
FROM 
    order_reviews
WHERE 
    review_comment_message LIKE '%bom%' 
	OR review_comment_message LIKE '%otimo%'
    OR review_comment_message LIKE '%gostei%'
    OR review_comment_message LIKE '%recomendo%'
    OR review_comment_message LIKE '%excelente%';


#the average review score of the comments contaning the positive words
SELECT 
    AVG(review_score) AS pos_w_avg_score
FROM 
    positive_review;

-- pos_w_avg_score = 4.4907

#the average review score of the comments that NOT contaning the positive words

SELECT 
    #review_comment_message
    AVG(review_score) AS not_pos_w_avg_score
FROM 
    order_reviews
WHERE 
    review_id NOT IN
        (SELECT 
                review_id
		FROM positive_review);

-- not_pos_w_avg_score = 4.0117

#caculate the percent of the 5 score in each group
SELECT 
    COUNT(review_score)/13138 # 13138 is the total number of the comments  contains the positive words. 
FROM 
    positive_review
WHERE 
    review_score=5; #0.7162

SELECT 
    COUNT(review_score)/85233 #85233 is the total number of the comments not contains the positive words
FROM 
    order_reviews
WHERE 
    review_id
    NOT IN
        (SELECT 
		     review_id
         FROM positive_review)
    AND review_score=5; # 0.5536

-- CONCLUTION:
/*he average review score of the comments that NOT contaning the positive words is 4.0117, which is lower than these contains the positive words, which average score is 4.4907. Meanwhile, comments with  positive words have a higher percent of 5 score-71.62%. And the comments without positive words has 55.36% of 5 review score.
So the answer is yes, reviews containing positive words have a better score.*/

/*****
2.3 Considering only states having at least 30 reviews containing these words, what is the state with the highest score?
*****/

SELECT 
    DISTINCT State,
    no_re,
    AVG(review_score) OVER(PARTITION BY (State)) AS avg_review_state
FROM
    (SELECT 
         review_score,
         sb.State,
         COUNT(review_score) OVER(PARTITION BY (sb.State)) AS no_re
	FROM 
        order_reviews
		    LEFT JOIN 
        orders using (order_id)
            LEFT JOIN
		customers AS cu using(customer_id)
            LEFT JOIN
		geo AS ge ON cu.customer_zip_code_prefix = ge.zip_code_prefix
			LEFT JOIN 
        states_brazil AS sb ON ge.state = sb.Abbreviation
WHERE 
    review_id
    IN 
        (SELECT 
            review_id
        FROM 
            positive_review))t
        WHERE 
            no_re>=30
        ORDER BY 
            avg_review_state DESC
    LIMIT 2;
    
-- RESULT: The state 'Tocantins' has 40 reviews containing these words, and with the highest average review score: 4.7250.

/****
2.4 What is the state where there is a greater score change between all reviews and reviews containing positive words?
****/

#with positive words
DROP TABLE IF EXISTS with_posi_avg_score;
CREATE TEMPORARY TABLE with_posi_avg_score
SELECT 
    review_comment_message,
    sb.State,
    AVG(review_score) OVER (PARTITION BY (sb.State)) AS pos_w_avg_score
FROM 
    order_reviews
        JOIN 
    orders using (order_id)
        JOIN
	customers AS cu using(customer_id)
        JOIN
	geo AS ge ON cu.customer_zip_code_prefix = ge.zip_code_prefix
        JOIN 
	states_brazil AS sb ON ge.state = sb.Abbreviation
WHERE 
    review_id
        IN 
        (SELECT 
            review_id
        FROM 
            positive_review);

SELECT * FROM with_posi_avg_score;

#without positive words

DROP TABLE IF EXISTS all_posi_avg_score;

CREATE TEMPORARY TABLE all_posi_avg_score
SELECT 
    DISTINCT sb.State,
    AVG(review_score) OVER (PARTITION BY (sb.State)) AS all_pos_w_avg_score
FROM 
    order_reviews
        JOIN 
    orders using (order_id)
        JOIN
	customers AS cu using(customer_id)
        JOIN
	geo AS ge ON cu.customer_zip_code_prefix = ge.zip_code_prefix
        JOIN 
	states_brazil AS sb ON ge.state = sb.Abbreviation;

SELECT 
    DISTINCT State,
    (pos_w_avg_score - all_pos_w_avg_score) AS score_change
FROM 
    with_posi_avg_score
        JOIN 
    all_posi_avg_score using(State)
ORDER BY 
    score_change DESC;

-- RESULTS: State Roraima has the greates score change (1.1056) between all reviews and reviews containing positive words.   

/*****
CHALLENGE 3
*****/

DROP PROCEDURE IF EXISTS avg_review_score;

DELIMITER $$

CREATE PROCEDURE avg_review_score(
                                  state VARCHAR(100),
                                  categoty VARCHAR(200) ,
								  the_year INT)
                                  
BEGIN

SELECT 
    avg_sore
FROM
    (SELECT 
	    DISTINCT sb.State,
		AVG(review_score) OVER(PARTITION BY(sb.State)) AS avg_sore,
        COUNT(product_id) OVER(PARTITION BY(sb.State)) AS no_product
    FROM 
	    order_reviews
            LEFT JOIN 
        orders using (order_id)
            LEFT JOIN
        order_items using(order_id)
            LEFT JOIN
        products using(product_id)
			LEFT JOIN
	    product_category_name_translation using(product_category_name)
            LEFT JOIN
        customers AS cu using(customer_id)
            LEFT JOIN
        geo AS ge ON cu.customer_zip_code_prefix = ge.zip_code_prefix
            LEFT JOIN 
        states_brazil AS sb ON ge.state = sb.Abbreviation
WHERE 
    sb.State = state
    AND 
    order_status = 'delivered'
    AND YEAR(order_purchase_timestamp) = the_year
    AND product_category_name_english = categoty)T
WHERE 
    no_product >= 1;

END$$

DELIMITER ;

CALL avg_review_score('Rio de Janeiro','health_beauty','2017'); -- 4.0854
CALL avg_review_score('Para','others','2017');-- 4.2500
