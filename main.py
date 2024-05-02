import functions_framework
from flask import jsonify


@functions_framework.http
def process_treatment(request):
    if request.method != 'POST':
        return jsonify(error="Invalid request. Only POST requests are allowed."), 400
    request_json = request.get_json(silent=True)
    if not request_json:
        return jsonify(error="Invalid request. No JSON body found."), 400
    previous_optimal_cpa = request_json.get('previous_optimal_cpa')
    optimal_cpa = request_json.get('optimal_cpa')
    previous_budget = request_json.get('previous_budget')
    budget = request_json.get('budget')
    real_cpa = request_json.get('real_cpa')
    ep_before_move = request_json.get('ep_before_move')
    ep_after_move = request_json.get('ep_after_move')
    r_amount_unit_micros = request_json.get('r_amount_unit_micros', 1000)

    if previous_optimal_cpa and optimal_cpa and previous_budget and budget and real_cpa and ep_before_move and ep_after_move:
        try:
            return decide(
                previous_optimal_cpa,
                optimal_cpa,
                previous_budget,
                budget,
                real_cpa,
                ep_before_move,
                ep_after_move,
                r_amount_unit_micros), 200
        except Exception as e:
            return jsonify(error=str(e)), 400

    return jsonify(error="Invalid data. Data uncompleted."), 400


PROFIT_MOVEMENT = ['stable', 'increase', 'decrease', 'unstable']
COEFFICIENT = 5  # in %
INCREASE_COEFFICIENT = 1.02
DECREASE_COEFFICIENT = 0.98


def detect_profit_movement(ep_before_move, ep_after_move):
    ep_ref_marging = ep_before_move * COEFFICIENT / 100
    ep_born_up = ep_before_move * (1 + ep_ref_marging)
    ep_born_down = ep_before_move * (1 - ep_ref_marging)

    if ep_born_down <= ep_after_move <= ep_born_up:
        return PROFIT_MOVEMENT[0]
    elif ep_after_move > ep_born_up:
        return PROFIT_MOVEMENT[1]
    elif ep_after_move < ep_born_down:
        return PROFIT_MOVEMENT[2]


def decide(
        previous_optimal_cpa: float,
        optimal_cpa: float,
        previous_budget: float,
        budget: float,
        real_cpa: float,
        ep_before_move: float,
        ep_after_move: float,
        r_amount_unit_micros: int
):
    profit_movement = detect_profit_movement(ep_before_move, ep_after_move)
    increased_optimal_cpa = optimal_cpa * INCREASE_COEFFICIENT
    decreased_optimal_cpa = optimal_cpa * DECREASE_COEFFICIENT
    increased_budget = budget * INCREASE_COEFFICIENT
    decreased_budget = budget * DECREASE_COEFFICIENT

    decision = {}
    # if previous move is to incerase optimal CPA OR if (optimal CPA is not changed & budget increase)
    if previous_optimal_cpa < optimal_cpa or (previous_optimal_cpa != optimal_cpa and previous_budget < budget):
        # If profit increases
        if profit_movement == PROFIT_MOVEMENT[1]:
            if optimal_cpa == real_cpa:
                decision['action_optimal_cpa'] = 'increase optimal CPA'
                decision['new_optimal_cpa'] = increased_optimal_cpa
                decision['action_budget'] = 'increase budget'
                decision['new_budget'] = increased_budget
            elif real_cpa < optimal_cpa:
                decision['action_optimal_cpa'] = 'optimal CPA not changed'
                decision['new_optimal_cpa'] = optimal_cpa
                decision['action_budget'] = 'increase budget'
                decision['new_budget'] = increased_budget
            elif optimal_cpa < real_cpa:
                decision['action_optimal_cpa'] = 'increase optimal CPA'
                decision['new_optimal_cpa'] = increased_optimal_cpa
                decision['action_budget'] = 'budget not changed'
                decision['new_budget'] = budget

        # profit is stable
        elif profit_movement == PROFIT_MOVEMENT[0]:
            if optimal_cpa == real_cpa:
                decision['action_optimal_cpa'] = 'decrease optimal CPA'
                decision['new_optimal_cpa'] = decreased_optimal_cpa
                decision['action_budget'] = 'decrease budget'
                decision['new_budget'] = decreased_budget
            elif optimal_cpa > real_cpa:
                decision['action_optimal_cpa'] = 'optimal CPA not changed'
                decision['new_optimal_cpa'] = optimal_cpa
                decision['action_budget'] = 'increase budget'
                decision['new_budget'] = increased_budget
            elif optimal_cpa < real_cpa:
                decision['action_optimal_cpa'] = 'optimal CPA not change'
                decision['new_optimal_cpa'] = optimal_cpa
                decision['action_budget'] = 'decrease budget'
                decision['new_budget'] = decreased_budget

        #  profit decreased
        elif profit_movement == PROFIT_MOVEMENT[2]:
            decision['action_optimal_cpa'] = 'decrease optimal CPA'
            decision['new_optimal_cpa'] = decreased_optimal_cpa
            decision['action_budget'] = 'decrease budget'
            decision['new_budget'] = decreased_budget

    # if previous move is to reduce optimal CPA OR if(optimal CPA is not changed & budget decrease)
    elif previous_optimal_cpa > optimal_cpa or (previous_optimal_cpa == optimal_cpa and previous_budget < budget):
        #  profit increases
        if profit_movement == PROFIT_MOVEMENT[1]:
            if real_cpa <= optimal_cpa:
                decision['action_optimal_cpa'] = 'decrease optimal CPA'
                decision['new_optimal_cpa'] = decreased_optimal_cpa
                decision['action_budget'] = 'decrease budget'
                decision['new_budget'] = decreased_budget
            elif real_cpa > optimal_cpa:
                decision['action_optimal_cpa'] = 'optimal CPA not change'
                decision['new_optimal_cpa'] = optimal_cpa
                decision['action_budget'] = 'decrease budget'
                decision['new_budget'] = decreased_budget

        elif profit_movement == PROFIT_MOVEMENT[0]:
            if optimal_cpa == real_cpa:
                decision['action_optimal_cpa'] = 'increase optimal CPA'
                decision['new_optimal_cpa'] = increased_optimal_cpa
                decision['action_budget'] = 'increase budget'
                decision['new_budget'] = increased_budget

            elif real_cpa > optimal_cpa:
                decision['action_optimal_cpa'] = 'optimal CPA not change'
                decision['new_optimal_cpa'] = optimal_cpa
                decision['action_budget'] = 'decrease budget'
                decision['new_budget'] = decreased_budget

            elif real_cpa < optimal_cpa:
                decision['action_optimal_cpa'] = ' optimal CPA not change'
                decision['new_optimal_cpa'] = optimal_cpa
                decision['action_budget'] = 'increase budget'
                decision['new_budget'] = increased_budget

        elif profit_movement == PROFIT_MOVEMENT[2]:
            decision['action_optimal_cpa'] = 'increase optimal CPA'
            decision['new_optimal_cpa'] = increased_optimal_cpa
            decision['action_budget'] = 'increase budget'
            decision['new_budget'] = increased_budget

    if decision['new_budget'] and decision['new_optimal_cpa']:
        cpa_micros = round(decision['new_optimal_cpa'] * 1000000)
        budget_micros = round(decision['new_budget'] * 1000000)
        decision['new_optimal_cpa_micros'] = cpa_micros - (cpa_micros % r_amount_unit_micros)
        decision['new_budget_micros'] = budget_micros - (budget_micros % r_amount_unit_micros)
    return decision
