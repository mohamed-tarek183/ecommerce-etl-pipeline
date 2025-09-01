CREATE TABLE IF NOT EXISTS core.dim_payment (
                    junk_PK INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    payment varchar(100),
                    loyalty_card Boolean,
                    UNIQUE (payment, loyalty_card)
                );
