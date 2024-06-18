create table paolaverdes_coderhouse.airquality_index (
	level VARCHAR(256),
	min_val INTEGER,
	max_val INTEGER,
	description VARCHAR(256));
insert into paolaverdes_coderhouse.airquality_index (level, min_val, max_val, description) values
('Good', 0, 50, 'Air quality is satisfactory and air pollution poses little or no risk.');
insert into paolaverdes_coderhouse.airquality_index (level, min_val, max_val, description) values
('Moderate', 51, 100, 'Air quality is acceptable. However, they may be a risk for some people, particularly those who are unusually sensitive to air pollution.');
insert into paolaverdes_coderhouse.airquality_index (level, min_val, max_val, description) values
('Unhelthy for sensitive groups', 101, 150, 'Members of sensitive groups may experience health affects. The general public is less likely to be affected.' );
insert into paolaverdes_coderhouse.airquality_index (level, min_val, max_val, description) values
('Unhelthy', 151, 200, 'Some members of general public may experience health effects; members of sensitive groups may experience more serious health effects.' );
insert into paolaverdes_coderhouse.airquality_index (level, min_val, max_val, description) values
('Very Unhelthy', 201, 300, 'Health alert: The risk of health effects is increased for everyone.' );
insert into paolaverdes_coderhouse.airquality_index (level, min_val, max_val, description) values
('Hazardous', 301, 99999, 'Health warning of emergency conditions: everyone is more likely to be affected.' );
