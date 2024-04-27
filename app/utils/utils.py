
class Utils:

    @staticmethod
    # Function to format the number
    def format_number(num):
        # Remove commas
        num = num.replace(',', '')
        # Split the number by the decimal point
        parts = num.split('.')
        # If there is only one part, it means there is no decimal point
        if len(parts) == 1:
            # Add a comma after every third digit from the right
            num = num[::-1]
            num = '.'.join([num[i:i + 3] for i in range(0, len(num), 3)])
            num = num[::-1]
        else:
            # Add a comma after every third digit from the right in the integer part
            integer_part = parts[0][::-1]
            integer_part = '.'.join([integer_part[i:i + 3] for i in range(0, len(integer_part), 3)])
            integer_part = integer_part[::-1]
            # Combine the integer part and the decimal part
            num = integer_part + ',' + parts[1]
        return num
