"""Finish all TODO items in this file to complete the isolation project, then
test your agent's strength against a set of known agents using tournament.py
and include the results in your report.
"""
import random



class SearchTimeout(Exception):
    """Subclass base exception for code clarity. """
    pass


def custom_score(game, player):
    """Calculate the heuristic value of a game state from the point of view
    of the given player.

    This should be the best heuristic function for your project submission.

    Note: this function should be called from within a Player instance as
    `self.score()` -- you should not need to call this function directly.

    Parameters
    ----------
    game : `isolation.Board`
        An instance of `isolation.Board` encoding the current state of the
        game (e.g., player locations and blocked cells).

    player : object
        A player instance in the current game (i.e., an object corresponding to
        one of the player objects `game.__player_1__` or `game.__player_2__`.)

    Returns
    -------
    float
        The heuristic value of the current game state to the specified player.
    """
    #TODO: finish this function!
    #raise NotImplementedError

    # ******  IMPROVED_SCORE_WITH_NO_NEG
    
    #check to see if game has been won
    if game.is_winner(player):
        return float("inf")

    # check to see if game has been lost
    if game.is_loser(player):
        return float("-inf")

    own_moves = len(game.get_legal_moves())
    opp_moves = len(game.get_legal_moves(game.get_opponent(player)))
    
    if own_moves > opp_moves:
        return float(own_moves)
    else:
        return float(0)
    
    
from random import randint
def custom_score_2(game, player):
    # ******  IMPROVED_SCORE_ABSOLUTE
    """Calculate the heuristic value of a game state from the point of view
    of the given player.

    Note: this function should be called from within a Player instance as
    `self.score()` -- you should not need to call this function directly.

    Parameters
    ----------
    game : `isolation.Board`
        An instance of `isolation.Board` encoding the current state of the
        game (e.g., player locations and blocked cells).

    player : object
        A player instance in the current game (i.e., an object corresponding to
        one of the player objects `game.__player_1__` or `game.__player_2__`.)

    Returns
    -------
    float
        The heuristic value of the current game state to the specified player.
    """   
    if game.is_winner(player):
        return float("inf")

    # check to see if game has been lost
    if game.is_loser(player):
        return float("-inf")

    own_moves = len(game.get_legal_moves())
    opp_moves = len(game.get_legal_moves(game.get_opponent(player)))
    
    
    return abs(float(own_moves - opp_moves))

    
    
        
def custom_score_3(game, player):
    # ******  CENTER_SCORE_WEIGHTED
    """Calculate the heuristic value of a game state from the point of view
    of the given player.

    Note: this function should be called from within a Player instance as
    `self.score()` -- you should not need to call this function directly.

    Parameters
    ----------
    game : `isolation.Board`
        An instance of `isolation.Board` encoding the current state of the
        game (e.g., player locations and blocked cells).

    player : object
        A player instance in the current game (i.e., an object corresponding to
        one of the player objects `game.__player_1__` or `game.__player_2__`.)

    Returns
    -------
    float
        The heuristic value of the current game state to the specified player.
    """
    # TODO: finish this function!
    if game.is_winner(player):
        return float("inf")

    # check to see if game has been lost
    if game.is_loser(player):
        return float("-inf")
    
    w, h = game.width / 2., game.height / 2.
    y, x = game.get_player_location(player)
    centre = float((h - y)**2 + (w - x)**2)
    if game.move_count >0  and game.move_count < 10  :
        return centre
    else:
        return 0.3 * centre
    #raise NotImplementedError


def custom_score_4(game, player):
    # ******  IMPROVED_SCORE_INVERSE
    # TODO: finish this function!
    if game.is_winner(player):
        return float("inf")

    # check to see if game has been lost
    if game.is_loser(player):
        return float("-inf")
  
    own_moves = len(game.get_legal_moves())
    opp_moves = len(game.get_legal_moves(game.get_opponent(player))) 
    
    if (own_moves - opp_moves) >0:
        return 1.0/float(own_moves - opp_moves)  
    else:
        return 0.0
   
    
def custom_score_5(game, player):
    # ******  CENTER_SCORE_WEIGHTED_INVERSE
    # TODO: finish this function!
    if game.is_winner(player):
        return float("inf")

    # check to see if game has been lost
    if game.is_loser(player):
        return float("-inf")
  
    w, h = game.width / 2., game.height / 2.
    y, x = game.get_player_location(player)
    centre = float((h - y)**2 + (w - x)**2)
    if game.move_count >0  and game.move_count < 10  :
        return 1.0/centre
    else:
        return 0.3 * 1.0/centre
    

def custom_score_6(game, player):
    # ******  CENTER_SCORE_INVERSE
    # TODO: finish this function!
    if game.is_winner(player):
        return float("inf")

    # check to see if game has been lost
    if game.is_loser(player):
        return float("-inf") 

    w, h = game.width / 2., game.height / 2.
    y, x = game.get_player_location(player)
    centre = float((h - y)**2 + (w - x)**2)
    return 1.0/centre
   
    
    
class IsolationPlayer:
    """Base class for minimax and alphabeta agents -- this class is never
    constructed or tested directly.

    ********************  DO NOT MODIFY THIS CLASS  ********************

    Parameters
    ----------
    search_depth : int (optional)
        A strictly positive integer (i.e., 1, 2, 3,...) for the number of
        layers in the game tree to explore for fixed-depth search. (i.e., a
        depth of one (1) would only explore the immediate sucessors of the
        current state.)

    score_fn : callable (optional)
        A function to use for heuristic evaluation of game states.

    timeout : float (optional)
        Time remaining (in milliseconds) when search is aborted. Should be a
        positive value large enough to allow the function to return before the
        timer expires.
    """
    def __init__(self, search_depth=3, score_fn=custom_score, timeout=10.):
        self.search_depth = search_depth
        self.score = score_fn
        self.time_left = None
        self.TIMER_THRESHOLD = timeout

        
class MinimaxPlayer(IsolationPlayer):
    """Game-playing agent that chooses a move using depth-limited minimax
    search. You must finish and test this player to make sure it properly uses
    minimax to return a good move before the search time limit expires.
    """

    def get_move(self, game, time_left):
        """Search for the best move from the available legal moves and return a
        result before the time limit expires.

        **************  YOU DO NOT NEED TO MODIFY THIS FUNCTION  *************

        For fixed-depth search, this function simply wraps the call to the
        minimax method, but this method provides a common interface for all
        Isolation agents, and you will replace it in the AlphaBetaPlayer with
        iterative deepening search.

        Parameters
        ----------
        game : `isolation.Board`
            An instance of `isolation.Board` encoding the current state of the
            game (e.g., player locations and blocked cells).

        time_left : callable
            A function that returns the number of milliseconds left in the
            current turn. Returning with any less than 0 ms remaining forfeits
            the game.

        Returns
        -------
        (int, int)
            Board coordinates corresponding to a legal move; may return
            (-1, -1) if there are no available legal moves.
        """

        self.time_left = time_left

        # Initialize the best move so that this function returns something
        # in case the search fails due to timeout
        best_move = (-1, -1)

        try:
            # The try/except block will automatically catch the exception
            # raised when the timer is about to expire.
            return self.minimax(game, self.search_depth)

        except SearchTimeout:
            pass  # Handle any actions required after timeout as needed

        # Return the best move from the last completed search iteration
        return best_move

    def minimax(self, game, depth):
        """Implement depth-limited minimax search algorithm as described in
        the lectures.

        This should be a modified version of MINIMAX-DECISION in the AIMA text.
        https://github.com/aimacode/aima-pseudocode/blob/master/md/Minimax-Decision.md

        **********************************************************************
            You MAY add additional methods to this class, or define helper
                 functions to implement the required functionality.
        **********************************************************************

        Parameters
        ----------
        game : isolation.Board
            An instance of the Isolation game `Board` class representing the
            current game state

        depth : int
            Depth is an integer representing the maximum number of plies to
            search in the game tree before aborting

        Returns
        -------
        (int, int)
            The board coordinates of the best move found in the current search;
            (-1, -1) if there are no legal moves

        Notes
        -----
            (1) You MUST use the `self.score()` method for board evaluation
                to pass the project tests; you cannot call any other evaluation
                function directly.

            (2) If you use any helper functions (e.g., as shown in the AIMA
                pseudocode) then you must copy the timer check into the top of
                each helper function or else your agent will timeout during
                testing.
        """
        # TODO: finish this function!           
        return self.max_value(game, depth)[0]   
    
           
        
    #my max_value
    def max_value(self, game, depth):
        if self.time_left() < self.TIMER_THRESHOLD:
            raise SearchTimeout()
            
        if depth == 0:
            return (game.get_player_location(self), self.score(game, self))
        
        value, best_move = float("-inf"), (-1, -1)
        
        for move in game.get_legal_moves():
            next_ply = game.forecast_move(move)
            score = self.min_value(next_ply, depth - 1)[1]
            if max(value, score) == score:
                best_move = move
                value = score

        return (best_move, value)

    #my min value
    def min_value(self, game, depth):
        if self.time_left() < self.TIMER_THRESHOLD:
            raise SearchTimeout()        
        
        if depth == 0:
            return (game.get_player_location(self), self.score(game, self))
        
        value, worst_move = float("inf"), (-1, -1)
        
        for move in game.get_legal_moves():
            next_ply = game.forecast_move(move)
            score = self.max_value (next_ply, depth - 1)[1]
            if min(value, score) == score:
                worst_move = move
                value = score

        return (worst_move, value)


class AlphaBetaPlayer(IsolationPlayer):
    """Game-playing agent that chooses a move using iterative deepening minimax
    search with alpha-beta pruning. You must finish and test this player to
    make sure it returns a good move before the search time limit expires.
    """

    def get_move(self, game, time_left):
        """Search for the best move from the available legal moves and return a
        result before the time limit expires.

        Modify the get_move() method from the MinimaxPlayer class to implement
        iterative deepening search instead of fixed-depth search.

        **********************************************************************
        NOTE: If time_left() < 0 when this function returns, the agent will
              forfeit the game due to timeout. You must return _before_ the
              timer reaches 0.
        **********************************************************************

        Parameters
        ----------
        game : `isolation.Board`
            An instance of `isolation.Board` encoding the current state of the
            game (e.g., player locations and blocked cells).

        time_left : callable
            A function that returns the number of milliseconds left in the
            current turn. Returning with any less than 0 ms remaining forfeits
            the game.

        Returns
        -------
        (int, int)
            Board coordinates corresponding to a legal move; may return
            (-1, -1) if there are no available legal moves.
        """
        self.time_left = time_left
        
        # TODO: finish this function!
        #raise NotImplementedError

        #move = (-1, -1)
        #for i in range(0, 1000):
        #    try:
        #        move = self.alphabeta(game, i)
        #    except SearchTimeout:
        #        break
        #return move


        move = (-1, -1)
        depth_counter = 1
        while True: #loop forevermore!
            try:
                move = self.alphabeta(game, depth_counter)
            except SearchTimeout:
                break               
            depth_counter += 1
            if depth_counter >= 999999999999:
                break
        return move
    


    def alphabeta(self, game, depth, alpha=float("-inf"), beta=float("inf")):
        """Implement depth-limited minimax search with alpha-beta pruning as
        described in the lectures.

        This should be a modified version of ALPHA-BETA-SEARCH in the AIMA text
        https://github.com/aimacode/aima-pseudocode/blob/master/md/Alpha-Beta-Search.md

        **********************************************************************
            You MAY add additional methods to this class, or define helper
                 functions to implement the required functionality.
        **********************************************************************

        Parameters
        ----------
        game : isolation.Board
            An instance of the Isolation game `Board` class representing the
            current game state

        depth : int
            Depth is an integer representing the maximum number of plies to
            search in the game tree before aborting

        alpha : float
            Alpha limits the lower bound of search on minimizing layers

        beta : float
            Beta limits the upper bound of search on maximizing layers

        Returns
        -------
        (int, int)
            The board coordinates of the best move found in the current search;
            (-1, -1) if there are no legal moves

        Notes
        -----
            (1) You MUST use the `self.score()` method for board evaluation
                to pass the project tests; you cannot call any other evaluation
                function directly.

            (2) If you use any helper functions (e.g., as shown in the AIMA
                pseudocode) then you must copy the timer check into the top of
                each helper function or else your agent will timeout during
                testing.
        """
        if self.time_left() < self.TIMER_THRESHOLD:
            raise SearchTimeout()

        # TODO: finish this function!        
        return self.max_value(game, depth, alpha=float("-inf"), beta=float("inf"))[0]
        

    def max_value (self, game, depth, alpha=float("-inf"), beta=float("inf")):

        if self.time_left() < self.TIMER_THRESHOLD:
            raise SearchTimeout()

        if depth == 0:
            return (-1, -1), self.score(game, self)

        value, best_move = float("-inf"), (-1, -1)
        
        for move in game.get_legal_moves():
            next_ply = game.forecast_move(move)
            score = self.min_value (next_ply, depth - 1, alpha, beta)[1]
            
            if score == max(value, score):
                value = score
                best_move = move
                          
            if value >= beta:
                return best_move, value
            else:
                alpha = max(value, alpha)
    
        return best_move, value

        
        
    def min_value (self, game, depth, alpha=float("-inf"), beta=float("inf")):
        if self.time_left() < self.TIMER_THRESHOLD:
            raise SearchTimeout()

            
        if depth == 0:
            return (-1, -1), self.score(game, self)

        value, best_move = float("inf"), (-1, -1)
        
        for move in game.get_legal_moves():
            next_ply = game.forecast_move(move)
            score = self.max_value (next_ply, depth - 1, alpha, beta)[1]
            
            if score == min(value, score):
                value = score
                best_move = move
                
            if value <= alpha:
                return best_move, value
            else:
                beta = min(value, beta)
            
        return best_move, value
