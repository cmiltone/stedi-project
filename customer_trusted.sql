INSERT INTO "stedi"."customer_trusted" (customername, email, birthday, serialnumber, sharewithresearchasofdate)
SELECT customername, email, birthday, serialnumber, sharewithresearchasofdate
FROM "stedi"."customer_landing"
WHERE sharewithresearchasofdate IS NOT NULL;