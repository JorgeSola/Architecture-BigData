create table public.airbnb_listings(
	id int4 primary key not null,
	host_id int4 not null,
	host_name varchar(100),
	host_neighbourhood text,
	host_listings_count int4,
	city varchar(100),
	country varchar(50),
	property_type varchar(50),
	room_type varchar (50),
	price int4,
	security_deposit int4,
	cleaning_fee int4,
	number_of_reviews int4,
	review_scores_value int4,
	cancellation_policy varchar(30)
	);

create table public.flat_information(
	host_id int4 not null primary key,
	tv int4,
	cable_tv int4,
	kitchen int4,
	smoking_allowed int4,
	pets_allowed int4,
	heating int4,
	washer int4,
	dryer int4,
	checkin_24h int4);

create table public.airbnb_secondary_information(
host_id int4 not null primary key,
street text,
zipcode text,
country_code varchar(4),
latitude float,
longitude float,
bedrooms int4,
beds int,
weekly_price int4,
monthly_price int4,
minimum_nights int4,
review_scores_accuracy int4,
review_scores_cleanliness int4,
review_scores_checkin int4,
review_scores_communication int4,
review_scores_location int4
);