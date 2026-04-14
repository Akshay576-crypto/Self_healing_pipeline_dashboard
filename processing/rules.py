from loguru import logger


class RulesEngine:

    RULES = {
        "name": {
            "default": "Unknown"
        },
        "age": {
            "min": 18,
            "max": 60,
            "default": 25
        },
        "email": {
            "contains": "@",
            "invalid_action": "move_to_error"
        }
    }

    @staticmethod
    def apply_rules(record):
        fixed_record = record.copy()
        errors = []
        fixes = 0

        # NAME RULE
        if not fixed_record.get("name"):
            fixed_record["name"] = RulesEngine.RULES["name"]["default"]
            fixes += 1

        # AGE RULE
        age = fixed_record.get("age")
        if age is None or not isinstance(age, int):
            fixed_record["age"] = RulesEngine.RULES["age"]["default"]
            fixes += 1
        else:
            if age < RulesEngine.RULES["age"]["min"] or age > RulesEngine.RULES["age"]["max"]:
                fixed_record["age"] = RulesEngine.RULES["age"]["default"]
                fixes += 1

        # EMAIL RULE
        email = fixed_record.get("email")
        if not email or "@" not in email:
            errors.append("Invalid email")

        return fixed_record, errors, fixes